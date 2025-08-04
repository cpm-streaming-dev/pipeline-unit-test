import streamlit as st

from page_json_converter import page_json_converter
from page_test_cases import page_test_cases
from page_validation import page_validation


st.set_page_config(
    page_title="JSON to SQL Converter",
    page_icon="🔄",
    layout="wide"
)


def main():

    st.sidebar.title("🧭 Navigation")

    page = st.sidebar.radio(
        "Select Page:",
        ["🔄 JSON to SQL Converter", "📋 Test Cases Management", "🔍 Validation"],
        index=0
    )

    st.sidebar.header("📊 Current Data")

    if 'json_input' in st.session_state:
        st.sidebar.success("✅ JSON input loaded")
        st.sidebar.write(f"Cases: {len(st.session_state['json_input'])}")
    else:
        st.sidebar.info("ℹ️ No JSON input loaded")

    if 'test_cases_data' in st.session_state and st.session_state['test_cases_data']:
        st.sidebar.success("✅ Test cases loaded")
        st.sidebar.write(
            f"Test cases: {len(st.session_state['test_cases_data'])}")
    else:
        st.sidebar.info("ℹ️ No test cases loaded")

    if 'validation_results' in st.session_state:
        st.sidebar.success("✅ Validation completed")

    if page == "🔄 JSON to SQL Converter":
        page_json_converter()
    elif page == "📋 Test Cases Management":
        page_test_cases()
    elif page == "🔍 Validation":
        page_validation()


if __name__ == "__main__":
    main()
