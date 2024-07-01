import csv
import json
import os
import re

def load_csv_mapping(csv_path):
    """
    Load a CSV file and return a list of dictionaries mapping from old (entity, column) pairs
    to new (entity, column) pairs, filtering out invalid rows based on specified conditions.
    
    Parameters:
    - csv_path: Path to the CSV file.
    
    Returns:
    - A list of dictionaries with keys as 'old_tbl', 'old_col', 'new_tbl', 'new_col'.
    """
    mappings = []
    with open(csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        expected_columns = ['old_tbl', 'old_col', 'new_tbl', 'new_col']
        # Strip BOM from the column names if present
        fieldnames = [name.lstrip('\ufeff') for name in reader.fieldnames]
        if not all(col in fieldnames for col in expected_columns):
            raise ValueError(f"CSV file must contain the following columns: {', '.join(expected_columns)}")
        for row in reader:
            old_tbl, old_col, new_tbl, new_col = row['old_tbl'], row['old_col'], row['new_tbl'], row['new_col']
            if old_tbl and (new_tbl or (old_col and new_col)):
                mappings.append(row)
    return mappings

def update_dax_expression(expression, table_map=None, column_map=None):
    """
    Update DAX expressions based on table_map and/or column_map.
    
    Parameters:
    - expression: The DAX expression to update.
    - table_map: A dictionary mapping old table names to new table names.
    - column_map: A dictionary mapping old (table, column) pairs to new (table, column) pairs.
    
    Returns:
    - Updated DAX expression.
    """
    if table_map:
        def replace_table_name(match):
            full_match = match.group(0)
            quotes = match.group(1) or ''
            table_name = match.group(2) or match.group(3)  # Group 2 for quoted, Group 3 for unquoted
            
            if table_name in table_map:
                new_table = table_map[table_name]
                if ' ' in new_table and not quotes:
                    return f"'{new_table}'"
                return f"{quotes}{new_table}{quotes}"
            return full_match

        # Updated pattern to match both quoted and unquoted table names, avoiding those inside square brackets
        pattern = re.compile(r"(?<!\[)('+)?(\b[\w\s]+?\b)\1|\b([\w]+)\b(?!\])")
        expression = pattern.sub(replace_table_name, expression)

    if column_map:
        def replace_column_name(match):
            full_match = match.group(0)
            table_part = match.group(1)
            column_name = match.group(2)
            
            # Remove quotes from table name for lookup
            table_name = table_part.strip("'")
            
            if (table_name, column_name) in column_map:
                new_table, new_column = column_map[(table_name, column_name)]
                # Preserve original quoting style if no spaces in new table name
                if ' ' in new_table or table_part.startswith("'"):
                    table_part = f"'{new_table}'"
                else:
                    table_part = new_table
                return f"{table_part}[{new_column}]"
            return full_match

        # Pattern to match table[column], 'table'[column], or 'table name'[column]
        pattern = re.compile(r"('[A-Za-z0-9_ ]+'?|[A-Za-z0-9_]+)\[([A-Za-z0-9_]+)\]")
        expression = pattern.sub(replace_column_name, expression)

    return expression

def update_entity(data, table_map):
    """
    Update the "Entity" fields and DAX expressions in the JSON data based on the table_map.
    
    Parameters:
    - data: The JSON data to update.
    - table_map: A dictionary mapping old table names to new table names.
    
    Returns:
    - True if any updates were made, False otherwise.
    """
    updated = False

    def traverse_and_update(data):
        nonlocal updated
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "Entity" and value in table_map:
                    data[key] = table_map[value]
                    updated = True
                elif key == "entities":
                    for entity in value:
                        if "name" in entity and entity["name"] in table_map:
                            entity["name"] = table_map[entity["name"]]
                            updated = True
                        if "expression" in entity and isinstance(entity["expression"], str):
                            original_expression = entity["expression"]
                            entity["expression"] = update_dax_expression(original_expression, table_map=table_map)
                            if entity["expression"] != original_expression:
                                updated = True
                        traverse_and_update(entity)
                elif key == "expression" and isinstance(value, str):
                    original_expression = value
                    data[key] = update_dax_expression(original_expression, table_map=table_map)
                    if data[key] != original_expression:
                        updated = True
                else:
                    traverse_and_update(value)
        elif isinstance(data, list):
            for item in data:
                traverse_and_update(item)

    traverse_and_update(data)
    return updated

def update_property(data, column_map):
    """
    Update the "Property" fields in the JSON data based on the column_map and updated table names.
    
    Parameters:
    - data: The JSON data to update.
    - column_map: A dictionary mapping old (table, column) pairs to new (table, column) pairs.
    - table_map: A dictionary mapping old table names to new table names.
    
    Returns:
    - True if any updates were made, False otherwise.
    """
    updated = False

    def traverse_and_update(data):
        nonlocal updated
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ["Column", "Measure"]:
                    entity = value.get("Expression", {}).get("SourceRef", {}).get("Entity")
                    property = value.get("Property")
                    if entity and property:
                        if (entity, property) in column_map:
                            new_entity, new_property = column_map[(entity, property)]
                            value["Expression"]["SourceRef"]["Entity"] = new_entity
                            value["Property"] = new_property
                            updated = True
                elif key == "expression" and isinstance(value, str):
                    original_expression = value
                    value = update_dax_expression(original_expression, column_map=column_map)
                    if value != original_expression:
                        data[key] = value
                        updated = True
                elif key == "filter":
                    if "From" in value and "Where" in value:
                        from_entity = value["From"][0]["Entity"]
                        for condition in value["Where"]:
                            column = condition.get("Condition", {}).get("Not", {}).get("Expression", {}).get("In", {}).get("Expressions", [{}])[0].get("Column", {})
                            property = column.get("Property")
                            if property:
                                if (from_entity, property) in column_map:
                                    new_entity, new_property = column_map[(from_entity, property)]
                                    column["Property"] = new_property
                                    updated = True
                else:
                    traverse_and_update(value)
        elif isinstance(data, list):
            for item in data:
                traverse_and_update(item)

    traverse_and_update(data)
    return updated

def process_json_file(file_path, table_map, column_map):
    """
    Process a single JSON file, updating its content based on the table_map and column_map.
    
    Parameters:
    - file_path: Path to the JSON file.
    - table_map: A dictionary mapping old table names to new table names.
    - column_map: A dictionary mapping old (table, column) pairs to new (table, column) pairs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        entity_updated = update_entity(data, table_map)
        if entity_updated:
            print(f"Entity updated in file: {file_path}")
        property_updated = update_property(data, column_map)
        if property_updated:
            print(f"Property updated in file: {file_path}")
        if entity_updated or property_updated:
            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, indent=2)
    except json.JSONDecodeError:
        print(f"Error: Unable to parse JSON in file: {file_path}")
    except IOError as e:
        print(f"Error: Unable to read or write file: {file_path}. {str(e)}")

def update_json_files_in_directory(directory_path, csv_path):
    """
    Update all JSON files in a directory based on a CSV mapping.
    
    Parameters:
    - directory_path: Path to the directory containing JSON files.
    - csv_path: Path to the CSV file with the mapping.
    """
    try:
        mappings = load_csv_mapping(csv_path)
        
        table_map = {}
        column_map = {}
        
        for row in mappings:
            old_tbl, old_col, new_tbl, new_col = row['old_tbl'], row['old_col'], row['new_tbl'], row['new_col']
            if new_tbl and new_tbl != old_tbl:
                table_map[old_tbl] = new_tbl
            if old_col and new_col:
                effective_tbl = table_map.get(old_tbl, old_tbl)
                column_map[(effective_tbl, old_col)] = (effective_tbl, new_col)
        
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    process_json_file(file_path, table_map, column_map)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Example usage:
# update_json_files_in_directory('path_to_pbir_directory', 'path_to_csv_file')
