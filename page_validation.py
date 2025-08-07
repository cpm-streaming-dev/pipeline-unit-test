import uuid
import streamlit as st
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import time
from src.utils import compare_specific_fields, custom_from_dict, extract_test_case_number, flatten_dict


def create_kafka_consumer(config):
    """Create and configure Kafka consumer with Avro deserializers"""
    try:
        # Schema Registry configuration
        schema_registry_conf = {
            'url': config['schema.registry.url'],
            'basic.auth.user.info': config['basic.auth.user.info']
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Create deserializers
        key_avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            from_dict=custom_from_dict
        )

        value_avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            from_dict=custom_from_dict
        )

        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': config['bootstrap.servers'],
            'security.protocol': config.get('security.protocol', 'SASL_SSL'),
            'sasl.mechanisms': config.get('sasl.mechanisms', 'PLAIN'),
            'sasl.username': config['sasl.username'],
            'sasl.password': config['sasl.password'],
            'group.id': f'validation-consumer-{str(uuid.uuid4())[:8]}',
            'auto.offset.reset': config.get('auto.offset.reset', 'earliest'),
            'enable.auto.commit': False,
            'session.timeout.ms': config.get('session.timeout.ms', 45000),
            'key.deserializer': key_avro_deserializer,
            'value.deserializer': value_avro_deserializer
        }

        consumer = DeserializingConsumer(consumer_config)
        consumer.subscribe([config['topic_name']])

        return consumer
    except Exception as e:
        st.error(f"❌ Error creating consumer: {e}")
        return None


def validate_kafka_messages(test_cases, config, progress_callback=None, result_callback=None):
    """
    Validate Kafka messages against test cases
    Returns when all test cases are validated, max messages reached, or 10 sec timeout
    """
    consumer = create_kafka_consumer(config)
    if not consumer:
        return None

    # Convert test cases to expected format
    expected_cases = {}
    for case in test_cases:
        case_num = case.get('mfec_case')
        if case_num:
            expected_cases[case_num] = {
                'fields': case.get('fields', {}),
                'topic_name': case.get('topic_name', ''),
                'mfec_case': case_num
            }

    print(f"\n{'='*60}")
    print("Starting Test Case Based Kafka Consumer Validation...")
    print(f"Expected test cases: {list(expected_cases.keys())}")
    print(f"{'='*60}")

    message_count = 0
    max_messages = config.get('max_messages', 50)
    test_results = {}
    validated_cases = set()

    # Track validation progress
    total_expected_cases = len(expected_cases)
    timeout_count = 0
    max_timeout_cycles = 1  # 10 seconds timeout = 1 cycle of 10 sec poll

    try:
        while message_count < max_messages and len(validated_cases) < total_expected_cases:
            msg = consumer.poll(timeout=10.0)

            if msg is None:
                timeout_count += 1
                print(f"Waiting for messages... (timeout {timeout_count}/1)")
                if progress_callback:
                    progress_callback(
                        f"Waiting for messages... ({len(validated_cases)}/{total_expected_cases} cases validated) - timeout {timeout_count}/1")

                # End validation if no messages for 10 seconds
                if timeout_count >= max_timeout_cycles:
                    print("⏰ No messages received for 10 seconds. Ending validation.")
                    if progress_callback:
                        progress_callback(
                            f"Timeout reached. Validation ended with {len(validated_cases)}/{total_expected_cases} cases validated")
                    break
                continue

            # Reset timeout counter when message received
            timeout_count = 0

            if msg.error():
                print(f'❌ Consumer error: {msg.error()}')
                continue

            message_count += 1

            # Extract message data
            msg_key = msg.key()
            msg_value = msg.value()

            print(f"\n🔍 Processing message {message_count}")
            print(
                f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

            # Extract test case number from key or value
            test_case_num = extract_test_case_number(msg_key, msg_value)

            if test_case_num is None:
                print("⚠️  Could not extract test case number")
                continue

            print(f"🎯 Extracted test case number: {test_case_num}")

            # Check if this test case is expected and not already validated
            if test_case_num not in expected_cases:
                print(f"⚠️  Test case {test_case_num} not in expected cases")
                continue

            if test_case_num in validated_cases:
                print(
                    f"⚠️  Test case {test_case_num} already validated, skipping")
                continue

            expected_data = expected_cases[test_case_num]
            expected_fields = expected_data['fields']

            # Perform comparison
            is_match = compare_specific_fields(
                msg_key, msg_value, expected_fields, test_case_num)

            # Store result
            test_results[test_case_num] = {
                'passed': is_match,
                'expected_fields': expected_fields,
                'consumed_value': msg_value,
                'message_info': {
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg_key,
                    'timestamp': msg.timestamp()[1] if msg.timestamp()[0] != -1 else None
                },
                'validation_timestamp': time.time()
            }

            validated_cases.add(test_case_num)

            if is_match:
                print(f"✅ TEST_CASE_{test_case_num:02d}: PASSED")
            else:
                print(f"❌ TEST_CASE_{test_case_num:02d}: FAILED")

            # Update progress and results immediately
            if progress_callback:
                progress_status = f"Validated {len(validated_cases)}/{total_expected_cases} test cases"
                progress_callback(progress_status)

            # Call result callback immediately after each validation
            if result_callback:
                result_callback(test_results.copy())

            # Check if all test cases are validated
            if len(validated_cases) == total_expected_cases:
                print(f"🎉 All {total_expected_cases} test cases validated!")
                if progress_callback:
                    progress_callback(
                        f"✅ All {total_expected_cases} test cases validated successfully!")
                break

    except KeyboardInterrupt:
        print("\n⏹️ Validation interrupted by user")
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        if progress_callback:
            progress_callback(f"Error: {e}")
    finally:
        consumer.close()

    # Calculate final results
    matches = sum(1 for r in test_results.values() if r['passed'])
    mismatches = len(test_results) - matches
    missing_cases = set(expected_cases.keys()) - validated_cases

    final_results = {
        'test_results': test_results,
        'summary': {
            'total_messages_processed': message_count,
            'total_expected_cases': total_expected_cases,
            'validated_cases': len(validated_cases),
            'passed_cases': matches,
            'failed_cases': mismatches,
            'missing_cases': list(missing_cases),
            'success_rate': (matches / len(validated_cases)) * 100 if validated_cases else 0,
            'completion_rate': (len(validated_cases) / total_expected_cases) * 100 if total_expected_cases else 0
        },
        'validation_complete': len(validated_cases) == total_expected_cases,
        'validated_case_numbers': sorted(list(validated_cases))
    }

    return final_results


def run_kafka_validation(test_cases, config):
    """Run Kafka validation with Streamlit UI updates and immediate result display"""

    # Initialize session state for tracking
    if 'validation_progress' not in st.session_state:
        st.session_state['validation_progress'] = ""

    # Create containers for real-time updates
    status_container = st.empty()
    results_container = st.empty()

    def update_progress(message):
        st.session_state['validation_progress'] = message
        with status_container.container():
            if "timeout" in message.lower():
                st.warning(f"⏰ {message}")
            elif "error" in message.lower():
                st.error(f"❌ {message}")
            elif "validated successfully" in message:
                st.success(f"✅ {message}")
            else:
                st.info(f"🔄 {message}")

    def update_results(results):
        """Update results immediately as they come in"""
        st.session_state['validation_results'] = results
        # Display results in real-time
        with results_container.container():
            if results:
                display_validation_results(results, real_time=True)

    # Start validation
    update_progress("Starting Kafka consumer validation...")

    try:
        # Run validation
        final_results = validate_kafka_messages(
            test_cases,
            config,
            progress_callback=update_progress,
            result_callback=update_results
        )

        if final_results:
            st.session_state['validation_results'] = final_results
            st.session_state['validation_running'] = False

            # Final status update
            if final_results['validation_complete']:
                status_container.success(
                    "✅ Validation completed! All test cases processed.")
            else:
                summary = final_results['summary']
                if summary['validated_cases'] == 0:
                    status_container.error(
                        "❌ No test cases were validated. Check if messages contain expected test case identifiers.")
                else:
                    status_container.warning(
                        f"⚠️ Validation ended with {summary['validated_cases']}/{summary['total_expected_cases']} cases validated.")

            # Display final results (non-real-time mode for full functionality)
            with results_container.container():
                display_validation_results(final_results, real_time=False)

        else:
            status_container.error("❌ Validation failed to complete.")
            st.session_state['validation_running'] = False

    except Exception as e:
        status_container.error(f"❌ Validation error: {e}")
        st.session_state['validation_running'] = False


def display_validation_results(results, real_time=False):
    """Display validation results in Streamlit with immediate updates"""

    if not results:
        return

    # Handle both partial results (during real-time) and complete results
    if 'summary' in results:
        summary = results['summary']
        test_results = results.get('test_results', {})
    else:
        # This is a partial result during real-time validation
        test_results = results
        # Calculate summary on the fly
        total_cases = len(st.session_state.get('test_cases_data', []))
        validated_count = len(test_results)
        passed_count = sum(1 for r in test_results.values() if r['passed'])
        failed_count = validated_count - passed_count

        summary = {
            'total_expected_cases': total_cases,
            'validated_cases': validated_count,
            'passed_cases': passed_count,
            'failed_cases': failed_count,
            'success_rate': (passed_count / validated_count) * 100 if validated_count > 0 else 0,
            'completion_rate': (validated_count / total_cases) * 100 if total_cases > 0 else 0,
            'missing_cases': []
        }

    # Summary metrics
    if not real_time:
        st.header("📊 Validation Results")
    else:
        st.subheader("📊 Live Validation Results")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Cases",
            summary['total_expected_cases'],
            help="Total number of test cases expected"
        )

    with col2:
        st.metric(
            "Validated",
            summary['validated_cases'],
            delta=f"{summary['completion_rate']:.1f}%" if summary['completion_rate'] < 100 else "Complete!",
            delta_color="normal",
            help="Number of test cases successfully validated"
        )

    with col3:
        st.metric(
            "Passed",
            summary['passed_cases'],
            delta=f"{summary['success_rate']:.1f}%" if summary['validated_cases'] > 0 else None,
            delta_color="normal" if summary['success_rate'] >= 80 else "inverse",
            help="Number of test cases that passed validation"
        )

    with col4:
        st.metric(
            "Failed",
            summary['failed_cases'],
            delta=-
            summary['failed_cases'] if summary['failed_cases'] > 0 else "None",
            delta_color="inverse" if summary['failed_cases'] > 0 else "normal",
            help="Number of test cases that failed validation"
        )

    # Progress bar
    progress_value = summary['completion_rate'] / 100
    st.progress(progress_value,
                text=f"Validation Progress: {summary['completion_rate']:.1f}%")

    # Test case results - Show immediately as they come in
    if test_results:
        st.subheader("📋 Test Case Results")

        # Only show filters when NOT in real-time mode and validation is not running
        if not real_time and not st.session_state.get('validation_running', False):
            # Initialize filter state if not exists
            if 'show_filter' not in st.session_state:
                st.session_state['show_filter'] = "All"
            if 'sort_by' not in st.session_state:
                st.session_state['sort_by'] = "Test Case Number"

            # Filter options with stable keys
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                show_filter = st.selectbox(
                    "Show:",
                    ["All", "Passed Only", "Failed Only"],
                    index=["All", "Passed Only", "Failed Only"].index(
                        st.session_state['show_filter']),
                    key="show_filter_stable"
                )
                st.session_state['show_filter'] = show_filter

            with filter_col2:
                sort_by = st.selectbox(
                    "Sort by:",
                    ["Test Case Number", "Status"],
                    index=["Test Case Number", "Status"].index(
                        st.session_state['sort_by']),
                    key="sort_by_stable"
                )
                st.session_state['sort_by'] = sort_by

            # Filter and sort results
            filtered_results = test_results.copy()
            if show_filter == "Passed Only":
                filtered_results = {k: v for k,
                                    v in test_results.items() if v['passed']}
            elif show_filter == "Failed Only":
                filtered_results = {
                    k: v for k, v in test_results.items() if not v['passed']}

            # Sort results
            if sort_by == "Test Case Number":
                sorted_cases = sorted(filtered_results.keys())
            else:  # Sort by status
                sorted_cases = sorted(filtered_results.keys(), key=lambda x: (
                    not filtered_results[x]['passed'], x))
        else:
            # In real-time or during validation, just sort by test case number for consistency
            filtered_results = test_results
            sorted_cases = sorted(filtered_results.keys())

        # Single expander with selectbox for test cases
        if sorted_cases:
            with st.expander("🔍 Test Case Details", expanded=True):
                # Initialize selected case if not exists
                if 'selected_test_case' not in st.session_state:
                    st.session_state['selected_test_case'] = sorted_cases[0]

                # Create selectbox options as "Case 1", "Case 2", etc.
                case_options = [f"Case {case}" for case in sorted_cases]
                case_display_to_num = {
                    f"Case {case}": case for case in sorted_cases}

                # Find current selection index
                current_case_display = f"Case {st.session_state['selected_test_case']}"
                if current_case_display not in case_options:
                    current_case_display = case_options[0]
                    st.session_state['selected_test_case'] = sorted_cases[0]

                mode_key = "realtime" if real_time else "static"

                def on_case_change():
                    st.session_state['selected_test_case'] = case_display_to_num[
                        st.session_state[
                            f"test_case_selector_{mode_key}_{'_'.join(map(str, sorted_cases))}"]
                    ]

                selected_case_display = st.selectbox(
                    "Select Test Case:",
                    case_options,
                    index=case_options.index(current_case_display),
                    key=f"test_case_selector_{mode_key}_{'_'.join(map(str, sorted_cases))}",
                    # key="test_case_selector",
                    on_change=on_case_change
                )

                # Get the actual case number from the display name
                selected_case_num = st.session_state.get(
                    'selected_test_case', sorted_cases[0])

                # Display the selected test case result
                result = filtered_results[selected_case_num]

                # Show status header
                status_text = '✅ PASSED' if result['passed'] else '❌ FAILED'
                st.markdown(
                    f"### TEST_CASE_{selected_case_num:02d} - {status_text}")

                info_col, details_col = st.columns([1, 2])

                with info_col:
                    st.write("**Message Info:**")
                    msg_info = result['message_info']
                    st.write(f"- Partition: {msg_info['partition']}")
                    st.write(f"- Offset: {msg_info['offset']}")
                    if msg_info.get('timestamp'):
                        timestamp = time.strftime('%Y-%m-%d %H:%M:%S',
                                                  time.localtime(msg_info['timestamp']/1000))
                        st.write(f"- Timestamp: {timestamp}")

                    # Show validation time in real-time mode
                    if real_time and 'validation_timestamp' in result:
                        val_time = time.strftime('%H:%M:%S',
                                                 time.localtime(result['validation_timestamp']))
                        st.write(f"- Validated at: {val_time}")

                with details_col:
                    st.write("**Expected Fields:**")
                    st.json(result['expected_fields'])

                    st.write("**Consumed Fields:**")
                    # Extract only the fields that were compared
                    consumed_relevant = {}
                    for field in result['expected_fields'].keys():
                        if field in result['consumed_value']:
                            consumed_relevant[field] = result['consumed_value'][field]
                    st.json(consumed_relevant)

                    # Show status with color coding
                    if result['passed']:
                        st.success(
                            "✅ **VALIDATION PASSED** - All fields match expected values")
                    else:
                        st.error(
                            "❌ **VALIDATION FAILED** - Fields do not match expected values")

    # Missing cases warning (only for complete results)
    if 'missing_cases' in summary and summary['missing_cases']:
        st.warning(
            f"⚠️ Missing test cases (not found in consumed messages): {summary['missing_cases']}")

    # Export results (only for complete results and when not running)
    if not real_time and test_results and 'summary' in results and not st.session_state.get('validation_running', False):
        if st.button("📥 Export Results to JSON", key="export_results_btn"):
            import json
            results_json = json.dumps(results, indent=2, default=str)
            st.download_button(
                label="Download Results",
                data=results_json,
                file_name=f"kafka_validation_results_{int(time.time())}.json",
                mime="application/json",
                key="download_results_btn"
            )


def page_validation():
    """Enhanced Page 3: Kafka Consumer Validation"""

    st.title("🔍 Kafka Consumer Validation")
    st.write("Run Kafka consumer to validate messages against test cases")

    # Check if test cases are loaded
    if 'test_cases_data' not in st.session_state or not st.session_state['test_cases_data']:
        st.warning("⚠️ No test cases loaded. Please upload test cases first.")
        if st.button("Go to Test Cases Management"):
            st.session_state['page_redirect'] = "test_cases_uploader"
            st.rerun()
        return

    # Display test cases summary
    test_cases = st.session_state['test_cases_data']
    st.success(
        f"✅ {len(test_cases)} test cases loaded and ready for validation")

    # Test cases overview
    with st.expander("📋 View Test Cases"):
        for case in test_cases:
            case_num = case.get('mfec_case', 'N/A')
            topic = case.get('topic_name', 'N/A')
            fields = case.get('fields', {})
            st.write(
                f"**Test Case {case_num}** (Topic: {topic}): {list(fields.keys())}")

    # Kafka Configuration Section
    st.header("⚙️ Kafka Configuration")

    # Configuration tabs
    config_tab1, config_tab2 = st.tabs(["📁 Config File", "📝 Manual Config", ])

    kafka_config = {}

    with config_tab1:
        st.subheader("Upload Kafka Config File")

        config_file = st.file_uploader(
            "Upload kafka_config.ini file",
            type=['ini', 'cfg', 'conf', 'toml'],
            help="Upload your Kafka configuration file"
        )

        if config_file is not None:
            try:
                file_extension = config_file.name.split('.')[-1].lower()

                if file_extension == 'toml':
                    # Handle TOML files
                    import tomli
                    file_content = config_file.getvalue().decode('utf-8')
                    config_dict = tomli.loads(file_content)

                    st.write("**Config file sections found:**")
                    for section in config_dict.keys():
                        st.write(f"- {section}")

                    is_cloud = st.radio(
                        "Environment:", ["Cloud", "On-Premises"], horizontal=True)
                    section_name = 'cloud' if is_cloud == 'Cloud' else 'onprem'

                    if section_name in config_dict:
                        raw_config = config_dict[section_name]
                        kafka_config = flatten_dict(raw_config)

                        # Convert boolean strings
                        for key, value in kafka_config.items():
                            if str(value).lower() in ['true', 'false']:
                                kafka_config[key] = str(
                                    value).lower() == 'true'

                        kafka_config.update({
                            'topic_name': st.text_input("Topic Name:", value="", key="file_topic"),
                            'max_messages': st.number_input("Max Messages:", min_value=1, max_value=100, value=50, key="file_max_msg")
                        })

                        if st.button("Use Config File"):
                            st.session_state['kafka_config'] = kafka_config
                            st.success("✅ Configuration loaded from file!")
                            st.json(kafka_config)

                else:
                    # Handle INI files - Fixed version
                    import configparser
                    from io import StringIO

                    config_content = StringIO(
                        config_file.getvalue().decode('utf-8'))
                    config = configparser.ConfigParser()
                    config.read_file(config_content)

                    st.write("**Config file sections found:**")
                    for section in config.sections():
                        st.write(f"- {section}")

                    is_cloud = st.radio(
                        "Environment:", ["Cloud", "On-Premises"], horizontal=True)
                    section_name = 'cloud' if is_cloud == 'Cloud' else 'onprem'

                    if section_name in config.sections():
                        # FIX: Properly convert values
                        kafka_config = {}
                        for key, value in dict(config[section_name]).items():
                            # Remove extra quotes and convert types
                            clean_value = value.strip('"\'')

                            # Convert boolean strings
                            if clean_value.lower() in ['true', 'false']:
                                kafka_config[key] = clean_value.lower(
                                ) == 'true'
                            else:
                                kafka_config[key] = clean_value

                        kafka_config.update({
                            'topic_name': st.text_input("Topic Name:", value="", key="file_topic"),
                            'max_messages': st.number_input("Max Messages:", min_value=1, max_value=100, value=50, key="file_max_msg")
                        })

                        if st.button("Use Config File"):
                            st.session_state['kafka_config'] = kafka_config
                            st.success("✅ Configuration loaded from file!")
                            st.json(kafka_config)  # Clean display
                    else:
                        st.error(
                            f"❌ Section '{section_name}' not found in config file")

            except Exception as e:
                st.error(f"❌ Error reading config file: {e}")

    # Validation Execution Section
    if 'kafka_config' in st.session_state:
        st.header("🚀 Run Validation")

        config = st.session_state['kafka_config']

        # Show current config
        with st.expander("📋 Current Kafka Configuration"):
            display_config = config.copy()
            for key in ['sasl.password', 'schema_registry_auth']:
                if key in display_config:
                    display_config[key] = "*" * 8
            st.json(display_config)

        # Validation controls
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("▶️ Start Validation", type="primary",
                         disabled=st.session_state.get('validation_running', False)):
                st.session_state['validation_running'] = True
                st.session_state['validation_results'] = None
                # Remove the immediate st.rerun() here to prevent widget conflicts
                # st.rerun()  # REMOVED THIS LINE

        with col2:
            if st.button("⏹️ Stop Validation"):
                st.session_state['validation_running'] = False
                st.rerun()

        with col3:
            if st.button("🗑️ Clear Results"):
                if 'validation_results' in st.session_state:
                    del st.session_state['validation_results']
                if 'validation_progress' in st.session_state:
                    del st.session_state['validation_progress']
                st.rerun()

        # Run validation if started (moved outside button click to prevent conflicts)
        if st.session_state.get('validation_running', False):
            run_kafka_validation(test_cases, config)

        # Display existing results if available and not currently running
        elif 'validation_results' in st.session_state and st.session_state['validation_results']:
            display_validation_results(
                st.session_state['validation_results'], real_time=False)
    with config_tab2:
        st.subheader("Kafka Connection Settings")

        col1, col2 = st.columns(2)

        with col1:
            bootstrap_servers = st.text_input(
                "Bootstrap Servers:",
                value=""
            )
            sasl_username = st.text_input(
                "SASL Username:", value="")
            topic_name = st.text_input("Topic Name:", value="MFEC_TD_STG_10")

        with col2:
            sasl_password = st.text_input(
                "SASL Password:", type="password",
                value=""
            )
            max_messages = st.number_input(
                "Max Messages to Process:", min_value=1, max_value=100, value=50
            )

        # Schema Registry Config
        st.subheader("Schema Registry Settings")
        col1, col2 = st.columns(2)

        with col1:
            schema_url = st.text_input(
                "Schema Registry URL:",
                value=""
            )

        with col2:
            schema_auth = st.text_input(
                "Schema Registry Auth (user:password):",
                value="KC7CLK26CED4PP4E:XaVdUMjfyHRDZ2GyTx+2+dAbCCWjyyZcCAbrGHEsbj1/QjZ5tRSudR2vcRD9ujOS"
            )

        if st.button("Save Manual Config"):
            if all([bootstrap_servers, sasl_username, sasl_password, schema_url, schema_auth]):
                kafka_config = {
                    'bootstrap.servers': bootstrap_servers,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'PLAIN',
                    'sasl.username': sasl_username,
                    'sasl.password': sasl_password,
                    'group.id': f'validation-consumer-{str(uuid.uuid4())[:8]}',
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': False,
                    'session.timeout.ms': 45000,
                    'schema.registry.url': schema_url,
                    'basic.auth.user.info': schema_auth,
                    'topic_name': topic_name,
                    'max_messages': max_messages
                }
                st.session_state['kafka_config'] = kafka_config
                st.success("✅ Kafka configuration saved!")
            else:
                st.error("❌ Please fill in all required fields")
