from decimal import Decimal
import json
from pprint import pprint
import re

from deepdiff import DeepDiff


def convert_decimals_to_float(obj):
    """Recursively convert Decimal objects to float in a dictionary or list"""
    if isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


def load_expected_results(file_path):
    """Load expected results from JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            expected_data = json.load(f)
        print(f"✓ Successfully loaded expected results from: {file_path}")

        # Create a mapping from mfec_case to expected fields
        case_mapping = {}
        for item in expected_data:
            case_id = item.get('mfec_case')
            fields = item.get('fields', {})
            case_mapping[case_id] = {
                'topic_name': item.get('topic_name'),
                'fields': fields
            }

        print(f"Loaded test cases: {list(case_mapping.keys())}")
        return case_mapping
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None


def extract_test_case_number(msg_key, msg_value):
    """Extract case number from TEST_CASE_XX format from any field in key or value"""

    def search_in_data(data, data_type):
        """Helper function to search through dictionary or string data"""
        if isinstance(data, dict):
            # Search through all fields in the dictionary
            for field_name, field_value in data.items():
                if field_value:
                    field_str = str(field_value)
                    # Look for values that start with TEST_CASE_
                    if field_str.startswith('TEST_CASE_'):
                        match = re.search(r'TEST_CASE_(\d+)', field_str)
                        if match:
                            print(
                                f"🔍 Found test case in {data_type} field '{field_name}': {field_str}")
                            return int(match.group(1))
        elif data:
            # Handle string data directly
            data_str = str(data)
            if data_str.startswith('TEST_CASE_'):
                match = re.search(r'TEST_CASE_(\d+)', data_str)
                if match:
                    print(f"🔍 Found test case in {data_type}: {data_str}")
                    return int(match.group(1))
        return None

    # First search in the key
    if msg_key:
        result = search_in_data(msg_key, "key")
        if result is not None:
            return result

    # If not found in key, search in the value
    if msg_value:
        result = search_in_data(msg_value, "value")
        if result is not None:
            return result

    print("⚠️  No TEST_CASE_XX pattern found in key or value")
    return None


def compare_specific_fields(consumed_key, consumed_value, expected_fields, test_case_num):
    """Compare only the specific fields defined in expected results

    Args:
        consumed_key: The key part of the consumed Kafka message
        consumed_value: The value part of the consumed Kafka message  
        expected_fields: Dictionary of expected field values
        test_case_num: Test case number for logging

    Returns:
        bool: True if all expected fields match, False otherwise
    """
    print(f"\n=== COMPARISON FOR TEST CASE {test_case_num} ===")

    print("Expected fields to check:")
    pprint(expected_fields)

    print("Consumed key:")
    pprint(consumed_key)

    print("Consumed value:")
    pprint(consumed_value)

    # Merge key and value into single dictionary
    # Value fields take precedence if there are duplicate keys
    merged_consumed = {}
    if consumed_key:
        merged_consumed.update(consumed_key)
    if consumed_value:
        merged_consumed.update(consumed_value)

    print("Merged consumed data (key + value):")
    pprint(merged_consumed)

    # Extract only the fields that should be compared
    consumed_fields_to_check = {}
    for field_name in expected_fields.keys():
        if field_name in merged_consumed:
            consumed_fields_to_check[field_name] = merged_consumed[field_name]
        else:
            print(f"⚠️  Field '{field_name}' not found in consumed message!")

    print("Consumed fields to check:")
    pprint(consumed_fields_to_check)

    # Perform comparison
    diff = DeepDiff(expected_fields, consumed_fields_to_check,
                    ignore_order=True)

    if not diff:
        print("✅ MATCH: เทสเคสผ่านจัฟ")
        return True
    else:
        print("❌ MISMATCH: Differences found:")
        pprint(diff)

        # Print specific differences in a readable format
        if 'values_changed' in diff:
            print("\n📝 Value differences:")
            for key, change in diff['values_changed'].items():
                print(
                    f"  {key}: expected={change['old_value']}, got={change['new_value']}")

        if 'dictionary_item_added' in diff:
            print("\n➕ Extra fields in consumed data:")
            for item in diff['dictionary_item_added']:
                print(f"  {item}")

        if 'dictionary_item_removed' in diff:
            print("\n➖ Missing fields in consumed data:")
            for item in diff['dictionary_item_removed']:
                print(f"  {item}")

        return False


def custom_from_dict(obj, ctx):
    """Custom deserializer that converts Decimals to floats"""
    return convert_decimals_to_float(obj)


def remove_schema_config_from_dict(conf):
    """Remove potential Schema Registry related configurations from dictionary"""

    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)

    return conf


def flatten_dict(nested_dict, parent_key='', sep='.'):
    """Flatten nested dictionary into dot-notation keys"""
    items = []
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))

    return dict(items)
