import os
import uuid
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import tomli

from utils import compare_specific_fields, custom_from_dict, extract_test_case_number, flatten_dict, load_expected_results, remove_schema_config_from_dict


def main():

    secrets_path = '.streamlit/secrets.toml'
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
    with open(secrets_path, 'rb') as f:
        config_dict = tomli.load(f)

    if 'type' not in config_dict:
        raise KeyError("'type' section not found in secrets.toml")

    if 'is_cloud' not in config_dict['type']:
        raise KeyError("'is_cloud' key not found in [type] section")
    is_cloud = str(config_dict['type']['is_cloud']).lower() == 'true'
    section_name = 'cloud' if is_cloud else 'onprem'

    if section_name not in config_dict:
        raise KeyError(f"'{section_name}' section not found in secrets.toml")

    raw_config = config_dict[section_name]

    flattened_config = flatten_dict(raw_config)

    schema_registry_conf = {
        'url': flattened_config['schema.registry.url'],
        'basic.auth.user.info': flattened_config['basic.auth.user.info']
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    key_avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        from_dict=custom_from_dict
    )

    value_avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        from_dict=custom_from_dict
    )

    flattened_config['group.id'] = f'validation-consumer-{str(uuid.uuid4())[:8]}'
    flattened_config['key.deserializer'] = key_avro_deserializer
    flattened_config['value.deserializer'] = value_avro_deserializer

    expected_file_path = "tests/expected/results.json"
    expected_cases = load_expected_results(expected_file_path)

    if expected_cases is None:
        print("Cannot proceed without expected data file.")
        return

    remove_schema_config_from_dict(flattened_config)

    consumer = DeserializingConsumer(flattened_config)
    consumer.subscribe(['MFEC_TD_STG_10'])

    print(f"\n{'='*60}")
    print("Starting Test Case Based Kafka Consumer Comparison...")
    print(f"{'='*60}")

    message_count = 0
    matches = 0
    mismatches = 0
    unmatched_cases = 0
    test_results = {}

    try:
        while message_count < 20:
            msg = consumer.poll(timeout=10.0)

            if msg is None:
                print("Waiting for messages...")
                continue

            if msg.error():
                print(f'❌ Consumer error: {msg.error()}')
                continue

            message_count += 1

            msg_key = msg.key()
            msg_value = msg.value()

            print(f"\n🔍 Processing message {message_count}")
            print(
                f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
            print(f"Key: {msg_key}")
            print(f"Value: {msg_value}")

            test_case_num = extract_test_case_number(msg_key, msg_value)

            if test_case_num is None:
                print("⚠️  Could not extract test case number from key")
                unmatched_cases += 1

                continue

            print(f"🎯 Extracted test case number: {test_case_num}")

            if test_case_num not in expected_cases:
                print(
                    f"⚠️  No expected data found for test case {test_case_num}")
                unmatched_cases += 1

                continue

            expected_data = expected_cases[test_case_num]
            expected_fields = expected_data['fields']

            is_match = compare_specific_fields(
                msg_key, msg_value, expected_fields, test_case_num)

            test_results[test_case_num] = {
                'passed': is_match,
                'expected_fields': expected_fields,
                'consumed_value': msg_value,
                'message_info': {
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg_key
                }
            }

            if is_match:
                matches += 1
                print(f"✅ TEST_CASE_{test_case_num:02d}: PASSED")
            else:
                mismatches += 1
                print(f"❌ TEST_CASE_{test_case_num:02d}: FAILED")

    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error during consumption: {e}")
    finally:
        print('close consumer...')
        consumer.close()

        print(f"\n{'='*60}")
        print("FINAL TEST RESULTS SUMMARY")
        print(f"{'='*60}")
        print(f"Total messages processed: {message_count}")
        print(f"✅ Test cases passed: {matches}")
        print(f"❌ Test cases failed: {mismatches}")
        print(f"⚠️  Unmatched cases: {unmatched_cases}")

        if message_count > 0:
            success_rate = (matches / (matches + mismatches)) * \
                100 if (matches + mismatches) > 0 else 0
            print(f"📊 Success rate: {success_rate:.1f}%")

        print(f"\n{'='*40}")
        print("DETAILED TEST CASE RESULTS")
        print(f"{'='*40}")

        for case_num in sorted(test_results.keys()):
            result = test_results[case_num]
            status = "PASSED ✅" if result['passed'] else "FAILED ❌"
            print(f"TEST_CASE_{case_num:02d}: {status}")
            if not result['passed']:
                print(f"  Expected: {result['expected_fields']}")
                print(
                    f"  Got relevant fields: {result.get('consumed_relevant_fields', 'N/A')}")

        expected_case_nums = set(expected_cases.keys())
        tested_case_nums = set(test_results.keys())
        missing_cases = expected_case_nums - tested_case_nums

        if missing_cases:
            print(
                f"\n⚠️  Missing test cases (not found in consumed messages): {sorted(missing_cases)}")


if __name__ == "__main__":
    main()
