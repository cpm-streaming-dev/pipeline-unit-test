from datetime import datetime
import json
import streamlit as st

from src.helpers import generate_sql_from_json


def page_json_converter():
    """Page 1: JSON to SQL Converter (your existing functionality)"""
    st.title("JSON to SQL Converter")
    st.write("Upload your JSON input and convert it to SQL INSERT statements")

    # Step 1: JSON Input
    st.header("📁 Step 1: Upload JSON Input")

    tab1, tab2 = st.tabs(["📁 File Upload", "📝 Text Input", ])

    json_data = None

    with tab1:
        st.subheader("Upload JSON file")

        uploaded_file = st.file_uploader(
            "Upload your JSON file", type=['json'])

        if uploaded_file is not None:
            try:
                json_data = json.loads(
                    uploaded_file.getvalue().decode('utf-8'))
                st.success("✅ JSON file loaded successfully!")
                st.json(json_data)
                st.session_state['json_input'] = json_data
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON format: {e}")
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")

    # Use data from session state if available
    if 'json_input' in st.session_state:
        json_data = st.session_state['json_input']

    # Step 2: Configuration
    if json_data:
        st.header("⚙️ Step 2: Configuration")

        col1, col2 = st.columns(2)

        with col1:
            platform = st.selectbox(
                "Select Platform:",
                ["KSQLDB", "FLINKSQL"],
                help="Choose the SQL platform for generation"
            )

        with col2:
            test_all_cases = st.checkbox(
                "Test All Cases",
                help="Generate SQL for all cases in the JSON"
            )

        # Case selection (only if not testing all cases)
        case_range = None
        case_list = None

        if not test_all_cases:
            st.subheader("Case Selection")

            # Get available case numbers
            available_cases = [case.get('mfec_case') for case in json_data if case.get(
                'mfec_case') is not None]
            available_cases.sort()

            st.write(f"Available cases: {available_cases}")

            selection_method = st.radio(
                "Select cases by:",
                ["Range", "Individual List"],
                horizontal=True
            )

            if selection_method == "Range":
                col1, col2 = st.columns(2)
                with col1:
                    start_case = st.number_input("Start case:", min_value=min(
                        available_cases), max_value=max(available_cases), value=min(available_cases))
                with col2:
                    end_case = st.number_input("End case:", min_value=min(
                        available_cases), max_value=max(available_cases), value=max(available_cases))
                case_range = (int(start_case), int(end_case))

            else:  # Individual List
                selected_cases = st.multiselect(
                    "Select individual cases:",
                    available_cases,
                    default=available_cases[:3] if len(
                        available_cases) >= 3 else available_cases
                )
                if selected_cases:
                    case_list = selected_cases

        # Step 3: Generate SQL
        st.header("🔄 Step 3: Generate SQL")

        if st.button("Generate SQL", type="primary"):
            try:
                sql_output = generate_sql_from_json(
                    json_data,
                    platform,
                    test_all_cases,
                    case_range,
                    case_list
                )

                if sql_output:
                    st.success("✅ SQL generated successfully!")

                    # Display SQL
                    st.subheader("Generated SQL:")
                    st.code(sql_output, language="sql")

                    # Download button
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"INSERT_INTO_SCRIPT_{timestamp}_{platform}.sql"

                    st.download_button(
                        label="📥 Download SQL File",
                        data=sql_output,
                        file_name=filename,
                        mime="text/plain",
                        type="secondary"
                    )

                    # Store in session state
                    st.session_state['generated_sql'] = sql_output
                    st.session_state['sql_filename'] = filename

                else:
                    st.warning(
                        "No SQL was generated based on current configuration.")

            except Exception as e:
                st.error(f"❌ Error generating SQL: {e}")
    with tab2:
        st.subheader("Paste JSON directly")

        json_input = st.text_area(
            "Paste your JSON here:",
            height=300,
            placeholder="""[
    {
        "mfec_case": 1,
        "pipeline_name": "MFEC_TD_STG_10",
        "fields": {
            "field_01": 1,
            "field_02": "gobe",
            "field_03": 123.45,
            "field_04": "TEST_CASE_01"
        }
    },
    {
        "mfec_case": 3,
        "pipeline_name": "MFEC_TD_STG_10",
        "fields": {
            "field_01": 4,
            "field_02": "wagyu",
            "field_03": 123.45,
            "field_04": "TEST_CASE_03"
        }
    }
]""",
            help="Paste your JSON input here"
        )

        if st.button("Parse JSON", type="primary"):
            if json_input.strip():
                try:
                    json_data = json.loads(json_input)
                    st.success("✅ JSON parsed successfully!")
                    st.json(json_data)
                    st.session_state['json_input'] = json_data
                except json.JSONDecodeError as e:
                    st.error(f"❌ Invalid JSON format: {e}")
            else:
                st.warning("Please enter some JSON data")
