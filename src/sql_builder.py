import configparser
import json
import os
from datetime import datetime

# --- Helper Function 1: จัดรูปแบบค่าสำหรับ SQL ---
def format_sql_value(value):
    """
    ฟังก์ชันสำหรับแปลงค่า Python data type ให้เป็น SQL format ที่ถูกต้อง
    """
    if isinstance(value, str):
        return f"'{value.replace("'", "''")}'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    return 'NULL'

# --- Helper Function 2: สร้าง INSERT INTO 1 บรรทัด ---
def build_single_insert(case):
    """
    สร้าง INSERT INTO statement จาก object ของ 1 เคส
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

# --- Main Logic ---
def main():
    """
    สคริปต์หลักที่จะอ่านไฟล์ config และ input
    เพื่อสร้าง SQL INSERT statements และ export เป็นไฟล์ .sql
    """
    # --- 1. อ่าน Config และ Input ---
    test_conf = configparser.ConfigParser()
    test_conf.read('config/test_config.ini', encoding='utf-8')
    # VVV เพิ่มบรรทัดนี้เพื่อ Debug VVV
    # print(f"DEBUG: Sections found in config file = {test_conf.sections()}") 

    try:
        with open('tests/inputs/cases.json', 'r', encoding='utf-8') as f:
            all_cases = json.load(f)
    except FileNotFoundError:
        print("Error: 'tests/inputs/cases.json' not found.")
        return

    # --- 2. เลือก Platform และเตรียมตัวแปร ---
    platform = test_conf.get('SETTINGS', 'PLATFORM', fallback='KSQLDB').upper()
    final_sql_output = "" # ตัวแปรสำหรับเก็บผลลัพธ์ SQL ทั้งหมด

    # --- 3. แยก Logic การสร้าง SQL ตาม Platform ---
    
    # ==================== KSQLDB LOGIC ====================
    if platform == 'KSQLDB':
        print("Platform: KSQLDB. Generating individual INSERT statements.")
        
        # กรองเคสตามเงื่อนไข
        cases_to_run = []
        if test_conf.getboolean('SETTINGS', 'TEST_ALL_CASES', fallback=False):
            cases_to_run = all_cases
        else:
            case_range_str = test_conf.get('SETTINGS', 'CASE_RANGE', fallback='').strip()
            if case_range_str:
                start, end = map(int, case_range_str.split('-'))
                cases_to_run = [case for case in all_cases if start <= case.get('mfec_case', -1) <= end]
            else:
                case_list_str = test_conf.get('SETTINGS', 'CASE_LIST', fallback='').strip()
                if case_list_str:
                    cases_set = {int(c.strip()) for c in case_list_str.split(',')}
                    cases_to_run = [case for case in all_cases if case.get('mfec_case') in cases_set]
        
        # สร้าง SQL ทีละบรรทัด
        for case in cases_to_run:
            final_sql_output += f"-- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n"
            final_sql_output += build_single_insert(case) + "\n\n"

# ==================== FlinkSQL LOGIC (แก้ไขใหม่) ====================
    elif platform == 'FLINKSQL':
        print("Platform: FlinkSQL. Logic will vary based on config.")
        
        if test_conf.getboolean('SETTINGS', 'TEST_ALL_CASES', fallback=False):
            print("Mode: TEST_ALL_CASES. Building one EXECUTE STATEMENT SET.")
            statements = []
            for case in all_cases:
                statements.append(f"  -- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n  " + build_single_insert(case))
            
            final_sql_output = "EXECUTE STATEMENT SET\nBEGIN\n"
            final_sql_output += "\n".join(statements)
            final_sql_output += "\nEND;"

        else:
            range_str = test_conf.get('SETTINGS', 'CASE_RANGE', fallback='').strip()
            list_str = test_conf.get('SETTINGS', 'CASE_LIST', fallback='').strip()
            
            # NEW LOGIC: ดึงเคสจาก list มาเก็บก่อนเสมอ
            list_cases = []
            list_set = set()
            if list_str:
                list_set = {int(i.strip()) for i in list_str.split(',')}
                list_cases = [c for c in all_cases if c.get('mfec_case') in list_set]

            # NEW LOGIC: ดึงเคสจาก range โดยต้องไม่ซ้ำกับเคสใน list
            range_cases = []
            if range_str:
                start, end = map(int, range_str.split('-'))
                # กรองเคสใน range ที่ "ไม่มี" อยู่ใน list_set
                range_cases = [c for c in all_cases if start <= c.get('mfec_case', -1) <= end and c.get('mfec_case') not in list_set]

            # --- Logic การสร้าง SQL ---
            if range_cases:
                print("Mode: CASE_RANGE found. Building EXECUTE STATEMENT SET for range, and individual INSERTs for list.")
                # สร้างส่วน SET สำหรับ Range
                range_statements = [f"  -- SQL for mfec_case: {c.get('mfec_case', 'N/A')}\n  " + build_single_insert(c) for c in range_cases]
                final_sql_output += "EXECUTE STATEMENT SET\nBEGIN\n"
                final_sql_output += "\n".join(range_statements)
                final_sql_output += "\nEND;\n\n"
            
            # สร้างส่วน INSERT เดี่ยวๆ สำหรับ List (จะทำงานเสมอถ้า list_cases มีค่า)
            if list_cases:
                # ถ้าไม่มี range_cases แต่มี list_cases ให้ print mode นี้
                if not range_cases:
                     print("Mode: CASE_LIST only. Building individual INSERT statements.")
                for case in list_cases:
                    final_sql_output += f"-- SQL for mfec_case: {case.get('mfec_case', 'N/A')}\n"
                    final_sql_output += build_single_insert(case) + "\n\n"
            
            if not range_cases and not list_cases:
                print("Warning: No cases selected for FlinkSQL.")
                
    else:
        print(f"Error: Platform '{platform}' is not supported. Please choose KSQLDB or FlinkSQL.")
        return

    if not final_sql_output.strip():
        print("No SQL was generated based on the current configuration.")
        return

    # --- 4. Export ผลลัพธ์ทั้งหมดลงไฟล์ ---
    output_dir = "tests/sql_builder"
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"INSERT_INTO_SCRIPT_{timestamp}_{platform}.sql"
    full_output_path = os.path.join(output_dir, output_filename)
    
    with open(full_output_path, 'w', encoding='utf-8') as f:
        f.write(final_sql_output.strip())
        
    print(f"\n✅ Successfully exported SQL to {full_output_path}")

if __name__ == '__main__':
    main()