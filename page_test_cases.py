from datetime import datetime
import json
import streamlit as st


def page_test_cases():
    """Page 2: Upload and Manage Test Cases"""
    st.title("📋 Test Cases Management")
    st.write("Upload test case JSON files for Kafka message validation")

    # Step 1: Upload Test Cases JSON
    st.header("📁 Step 1: Upload Test Cases")

    # Create tabs for different input methods
    tab1, tab2, tab3 = st.tabs(
        ["📁 File Upload", "📝 Text Input",  "➕ Manual Entry"])

    test_cases_data = None

    with tab1:
        st.subheader("Upload Test Cases JSON file")

        uploaded_test_file = st.file_uploader("Upload your test cases JSON file", type=[
                                              'json'], key="test_cases_uploader")

        if uploaded_test_file is not None:
            try:
                test_cases_data = json.loads(
                    uploaded_test_file.getvalue().decode('utf-8'))
                st.success("✅ Test cases JSON file loaded successfully!")
                st.json(test_cases_data)
                st.session_state['test_cases_data'] = test_cases_data
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON format: {e}")
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")

    with tab2:
        st.subheader("Paste Test Cases JSON directly")

        test_cases_input = st.text_area(
            "Paste your test cases JSON here:",
            height=400,
            placeholder="""[
  {
    "mfec_case": 1,
    "topic_name": "MFEC_TD_STG_10",
    "fields": {
      "field_03": 123.45
    }
  },
  {
    "mfec_case": 3,
    "topic_name": "MFEC_TD_STG_10",
    "fields": {
      "field_02": "wagyu",
      "field_03": 123.45
    }
  },
  {
    "mfec_case": 2,
    "topic_name": "MFEC_TD_STG_10",
    "fields": {
      "field_01": 5,
      "field_02": "kee",
      "field_03": 123.45,
      "field_04": "TEST_CASE_02"
    }
  }
]""",
            help="Paste your test cases JSON here"
        )

        if st.button("Parse Test Cases JSON", type="primary"):
            if test_cases_input.strip():
                try:
                    test_cases_data = json.loads(test_cases_input)
                    st.success("✅ Test cases JSON parsed successfully!")
                    st.json(test_cases_data)
                    st.session_state['test_cases_data'] = test_cases_data
                except json.JSONDecodeError as e:
                    st.error(f"❌ Invalid JSON format: {e}")
            else:
                st.warning("Please enter some JSON data")

    with tab3:
        st.subheader("Add Individual Test Case")

        # Initialize manual test cases in session state if not exists
        if 'manual_test_cases' not in st.session_state:
            st.session_state['manual_test_cases'] = []

        # Form to add new test case
        with st.form("add_manual_test_case_form"):
            col1, col2 = st.columns(2)

            with col1:
                mfec_case = st.number_input(
                    "MFEC Case Number:", min_value=1, value=1, step=1)
                topic_name = st.text_input(
                    "Topic Name:", value="MFEC_TD_STG_10")

            with col2:
                # Dynamic field count
                num_fields = st.number_input(
                    "Number of Fields:", min_value=1, max_value=10, value=3, step=1)

            # Dynamic fields section
            st.subheader("Expected Fields:")
            fields = {}

            # Create columns for field inputs
            for i in range(num_fields):
                col1, col2, col3 = st.columns([2, 2, 2])

                with col1:
                    field_name = st.text_input(
                        f"Field {i+1} Name:", value=f"field_{i+1:02d}", key=f"manual_field_name_{i}")

                with col2:
                    field_type = st.selectbox(
                        f"Field {i+1} Type:", ["string", "number", "boolean"], key=f"manual_field_type_{i}")

                with col3:
                    if field_type == "string":
                        field_value = st.text_input(
                            f"Expected Value:", value="", key=f"manual_field_value_{i}")
                    elif field_type == "number":
                        field_value = st.number_input(
                            f"Expected Value:", value=0.0, key=f"manual_field_value_{i}")
                    else:  # boolean
                        field_value = st.checkbox(
                            f"Expected Value:", key=f"manual_field_value_{i}")

                if field_name:
                    fields[field_name] = field_value

            # Form submit button
            submitted = st.form_submit_button(
                "➕ Add Test Case", type="primary")

            if submitted:
                # Check if case number already exists
                existing_cases = [
                    case.get('mfec_case') for case in st.session_state['manual_test_cases']]
                if mfec_case in existing_cases:
                    st.error(f"❌ Test case {mfec_case} already exists!")
                else:
                    # Create new test case
                    new_case = {
                        "mfec_case": mfec_case,
                        "topic_name": topic_name,
                        "fields": fields
                    }

                    st.session_state['manual_test_cases'].append(new_case)
                    st.success(f"✅ Test case {mfec_case} added successfully!")
                    st.rerun()

        # Display manually created test cases
        if st.session_state['manual_test_cases']:
            st.subheader("📋 Manually Created Test Cases")

            for i, case in enumerate(st.session_state['manual_test_cases']):
                with st.expander(f"Test Case {case.get('mfec_case', 'N/A')} - {case.get('topic_name', 'N/A')}"):
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        st.json(case)

                    with col2:
                        if st.button(f"🗑️ Delete", key=f"delete_manual_{i}", type="secondary"):
                            st.session_state['manual_test_cases'].pop(i)
                            st.rerun()

            # Use manual test cases
            if st.button("📋 Use Manual Test Cases", type="primary"):
                st.session_state['test_cases_data'] = st.session_state['manual_test_cases']
                st.success("✅ Manual test cases loaded!")
                st.rerun()

    # Use data from session state if available
    if 'test_cases_data' in st.session_state:
        test_cases_data = st.session_state['test_cases_data']

    # Step 2: Display and Manage Test Cases
    if test_cases_data:
        st.header("📊 Step 2: Test Cases Overview")

        # Summary information
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Test Cases", len(test_cases_data))

        with col2:
            case_numbers = [case.get('mfec_case')
                            for case in test_cases_data if case.get('mfec_case')]
            st.metric(
                "Case Numbers", f"{min(case_numbers)} - {max(case_numbers)}" if case_numbers else "N/A")

        with col3:
            topics = set(case.get('topic_name')
                         for case in test_cases_data if case.get('topic_name'))
            st.metric("Unique Topics", len(topics))

        # Display each test case
        st.subheader("📋 Test Case Details")

        for case in test_cases_data:
            case_num = case.get('mfec_case', 'Unknown')
            topic = case.get('topic_name', 'Unknown')
            fields = case.get('fields', {})

            with st.expander(f"Test Case {case_num} - Topic: {topic}"):
                col1, col2 = st.columns(2)

                with col1:
                    st.write("**Expected Fields:**")
                    for field_name, field_value in fields.items():
                        st.write(
                            f"- `{field_name}`: {field_value} ({type(field_value).__name__})")

                with col2:
                    st.write("**Raw JSON:**")
                    st.json(case)

        # Step 3: Export and Usage Options
        st.header("📤 Step 3: Export & Usage")

        col1, col2, col3 = st.columns(3)

        with col1:
            # Download as JSON
            json_output = json.dumps(test_cases_data, indent=2)
            st.download_button(
                label="📥 Download Test Cases JSON",
                data=json_output,
                file_name=f"test_cases_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                type="secondary"
            )

        with col2:
            # Clear test cases
            if st.button("🗑️ Clear Test Cases", type="secondary"):
                if 'test_cases_data' in st.session_state:
                    del st.session_state['test_cases_data']
                st.rerun()

        with col3:
            # Show usage info
            if st.button("💡 Usage Info", type="secondary"):
                st.info("""
                **How to use these test cases:**
                1. These test cases define expected results for Kafka message validation
                2. Each test case specifies which fields should match in consumed messages
                3. Use with your Kafka consumer to compare actual vs expected results
                4. Test cases use `topic_name` for validation, not SQL generation
                """)

    # Show current status
    st.sidebar.header("📋 Test Cases Status")
    if 'test_cases_data' in st.session_state:
        st.sidebar.success("✅ Test cases loaded")
        st.sidebar.write(f"Cases: {len(st.session_state['test_cases_data'])}")

        # Show case numbers
        case_nums = [case.get('mfec_case', 'N/A')
                     for case in st.session_state['test_cases_data']]
        st.sidebar.write(
            f"Case numbers: {', '.join(map(str, sorted(case_nums)))}")
    else:
        st.sidebar.info("ℹ️ No test cases loaded")
