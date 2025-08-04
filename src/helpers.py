import streamlit as st
# --- Helper Function 1: Format values for SQL ---


def format_sql_value(value):
    """
    Convert Python data type to correct SQL format
    """
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    return 'NULL'

# --- Helper Function 2: Build single INSERT statement ---


def build_single_insert(case):
    """
    Create INSERT INTO statement from a single case object
    """
    try:
        table_name = case["pipeline_name"]
        fields_data = case["fields"]
        columns = ", ".join(fields_data.keys())
        values = ", ".join([format_sql_value(v) for v in fields_data.values()])
        return f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    except (KeyError, AttributeError):
        case_id = case.get('mfec_case', 'N/A')
        return f"-- Skipping case {case_id}: Invalid format (missing 'pipeline_name' or 'fields').\n"

# --- SQL Generation Logic ---


def generate_sql_from_json(json_data, platform, test_all_cases=False, case_range=None, case_list=None):
    """
    Generate SQL based on the uploaded JSON and configuration
    """
    final_sql_output = ""

    if platform == 'KSQLDB':
        st.info("Platform: KSQLDB. Generating individual INSERT statements.")

        # Filter cases based on conditions
        cases_to_run = []
        if test_all_cases:
            cases_to_run = json_data
        else:
            if case_range:
                start, end = case_range
                cases_to_run = [case for case in json_data if start <= case.get(
                    'mfec_case', -1) <= end]
            elif case_list:
                cases_set = set(case_list)
                cases_to_run = [case for case in json_data if case.get(
                    'mfec_case') in cases_set]

        # Generate SQL line by line
        for case in cases_to_run:
            final_sql_output += f"-- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n"
            final_sql_output += build_single_insert(case) + "\n\n"

    elif platform == 'FLINKSQL':
        st.info("Platform: FlinkSQL. Logic varies based on config.")

        if test_all_cases:
            st.write("Mode: TEST_ALL_CASES. Building one EXECUTE STATEMENT SET.")
            statements = []
            for case in json_data:
                statements.append(
                    f"  -- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n  " + build_single_insert(case))

            final_sql_output = "EXECUTE STATEMENT SET\nBEGIN\n"
            final_sql_output += "\n".join(statements)
            final_sql_output += "\nEND;"

        else:
            # Get cases from list first
            list_cases = []
            list_set = set()
            if case_list:
                list_set = set(case_list)
                list_cases = [c for c in json_data if c.get(
                    'mfec_case') in list_set]

            # Get cases from range (excluding list cases)
            range_cases = []
            if case_range:
                start, end = case_range
                range_cases = [c for c in json_data if start <= c.get(
                    'mfec_case', -1) <= end and c.get('mfec_case') not in list_set]

            # Generate SQL
            if range_cases:
                st.write(
                    "Mode: CASE_RANGE found. Building EXECUTE STATEMENT SET for range, and individual INSERTs for list.")
                range_statements = [
                    f"  -- SQL for mfec_case: {c.get('mfec_case', 'N/A')}\n  " + build_single_insert(c) for c in range_cases]
                final_sql_output += "EXECUTE STATEMENT SET\nBEGIN\n"
                final_sql_output += "\n".join(range_statements)
                final_sql_output += "\nEND;\n\n"

            # Individual INSERTs for list cases
            if list_cases:
                if not range_cases:
                    st.write(
                        "Mode: CASE_LIST only. Building individual INSERT statements.")
                for case in list_cases:
                    final_sql_output += f"-- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n"
                    final_sql_output += build_single_insert(case) + "\n\n"

            if not range_cases and not list_cases:
                st.warning("No cases selected for FlinkSQL.")

    return final_sql_output.strip()
