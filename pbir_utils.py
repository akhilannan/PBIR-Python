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
                new_column = column_map[(table_name, column_name)]
                # Preserve original quoting style if no spaces in new table name
                if ' ' in table_name or table_part.startswith("'"):
                    table_part = f"'{table_name}'"
                else:
                    table_part = table_name
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
                            new_property = column_map[(entity, property)]
                            value["Expression"]["SourceRef"]["Entity"] = entity
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
                                    new_property = column_map[(from_entity, property)]
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
    - column_map: A dictionary mapping old (table, column) pairs to new column names.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        entity_updated = False
        property_updated = False

        if table_map:
            entity_updated = update_entity(data, table_map)
            if entity_updated:
                print(f"Entity updated in file: {file_path}")

        if column_map:
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
                column_map[(effective_tbl, old_col)] = new_col
        
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    process_json_file(file_path, table_map, column_map)
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def extract_report_name(json_file_path):
    """
    Extracts the report name from the JSON file path.

    Args:
        json_file_path (str): The file path to the JSON file.

    Returns:
        str: The extracted report name if found, otherwise "NA".
    """
    return next((component[:-7] for component in reversed(json_file_path.split(os.sep))
                 if component.endswith('.Report')), "NA")


def extract_active_section(bookmark_json_path):
    """
    Extracts the active section from the bookmarks JSON file.

    Args:
        bookmark_json_path (str): The file path to the bookmarks JSON file.

    Returns:
        str: The active section if found, otherwise an empty string.
    """
    if "bookmarks" in bookmark_json_path:
        try:
            with open(bookmark_json_path, 'r', encoding='utf-8') as file:
                return json.load(file).get("explorationState", {}).get("activeSection", "")
        except (IOError, json.JSONDecodeError):
            return ""
    else:
        parts = bookmark_json_path.split(os.sep)
        return parts[parts.index("pages") + 1] if "pages" in parts else ""


def extract_page_name(json_path):
    """
    Extracts the page name from the JSON file path.

    Args:
        json_path (str): The file path to the JSON file.

    Returns:
        str: The extracted page name if found, otherwise "NA".
    """
    active_section = extract_active_section(json_path)
    if not active_section:
        return "NA"
    base_path = json_path.split("definition")[0]
    page_json_path = os.path.join(base_path, "definition", "pages", active_section, "page.json")
    try:
        with open(page_json_path, "r", encoding='utf-8') as file:
            return json.load(file).get("displayName", "NA")
    except (IOError, json.JSONDecodeError):
        return "NA"


def extract_power_bi_metadata_to_csv(directory_path, csv_output_path):
    """
    Extracts metadata from Power BI report JSON files (PBIR) in a directory and writes it to a CSV file.

    This function processes JSON files representing Power BI reports, extracting information about
    tables, columns, measures, their expressions, and where they are used within the report. It
    handles multiple JSON files in the given directory, consolidating the extracted information
    into a single CSV output.

    Args:
        directory_path (str): The directory path containing Power BI report JSON files.
        csv_output_path (str): The output path for the CSV file containing the extracted metadata.

    Returns:
        None

    The resulting CSV file will contain the following columns:
    - Report: Name of the Power BI report
    - Page: Name of the page within the report (or "NA" if not applicable)
    - Table: Name of the table
    - Column or Measure: Name of the column or measure
    - Expression: DAX expression for measures (if applicable)
    - Used In: Context where the item is used (e.g., visual, Drillthrough, Filters, Bookmarks)
    """
    
    def traverse_json(data, context=None):
        """
        Recursively traverses the JSON data to extract specific values.

        Args:
            data (dict or list): The JSON data to traverse.
            context (str): The context in which the value is used.

        Yields:
            tuple: Extracted values in the form of (table, column, context, expression).
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "Entity":
                    yield (value, None, context, None)
                elif key == "Property":
                    yield (None, value, context, None)
                elif key == "visual":
                    yield from traverse_json(value, value.get("visualType", "visual"))
                elif key == "pageBinding":
                    yield from traverse_json(value, value.get("type", "Drillthrough"))
                elif key == "filterConfig":
                    yield from traverse_json(value, "Filters")
                elif key == "explorationState":
                    yield from traverse_json(value, "Bookmarks")
                elif key == "entities":
                    for entity in value:
                        table_name = entity.get("name")
                        for measure in entity.get("measures", []):
                            yield (table_name, measure.get("name"), context, measure.get("expression", None))
                else:
                    yield from traverse_json(value, context)
        elif isinstance(data, list):
            for item in data:
                yield from traverse_json(item, context)

    def extract_power_bi_metadata_from_json_files(directory_path):
        """
        Extracts Power BI metadata from all JSON files in the specified directory.

        Args:
            directory_path (str): The directory path containing Power BI report JSON files.

        Returns:
            list: A list of dictionaries, each containing extracted metadata for a single item
                  (table, column, or measure) from the Power BI reports.
        """

        # Extract data from all json files in a directory
        all_rows = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.json'):
                    json_file_path = os.path.join(root, file)
                    report_name = extract_report_name(json_file_path)
                    page_name = extract_page_name(json_file_path) or "NA"
                    try:
                        with open(json_file_path, 'r', encoding='utf-8') as file:
                            data = json.load(file)
                            for table, column, used_in, expression in traverse_json(data):
                                all_rows.append({"Report": report_name, "Page": page_name, "Table": table, "Column or Measure": column, "Expression": expression, "Used In": used_in})
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Error: Unable to process file {json_file_path}: {str(e)}")

        # Separate rows based on whether they have an "expression" value
        rows_with_expression = [row for row in all_rows if row['Expression'] is not None]
        rows_without_expression = [row for row in all_rows if row['Expression'] is None]

        # This step is done to ensure we get table and respective column in single row
        reformatted_rows = [
            {
                "Report": rows_without_expression[i]["Report"],
                "Page": rows_without_expression[i]["Page"],
                "Table": rows_without_expression[i]["Table"],
                "Column or Measure": rows_without_expression[i + 1]["Column or Measure"],
                "Expression": None,
                "Used In": rows_without_expression[i]["Used In"]
            }
            for i in range(0, len(rows_without_expression), 2)
            if i + 1 < len(rows_without_expression)
        ]

        # This step ensures we add expression to the reformatted_rows based on a join to rows_with_expression
        for row_without in reformatted_rows:
            for row_with in rows_with_expression:
                if (row_without['Report'] == row_with['Report'] and
                    row_without['Table'] == row_with['Table'] and
                    row_without['Column or Measure'] == row_with['Column or Measure']):
                    row_without['Expression'] = row_with['Expression']
                    break  # Stop looking once a match is found

        # Ensure rows_with_expression that were not used anywhere are added to reformatted_rows
        final_rows = reformatted_rows + [row for row in rows_with_expression if not any(
            row['Report'] == r['Report'] and
            row['Table'] == r['Table'] and
            row['Column or Measure'] == r['Column or Measure'] for r in reformatted_rows)]
        
        # Extract distinct rows
        unique_rows = []
        seen = set()
        for row in final_rows:
            row_tuple = (row['Report'], row['Page'], row['Table'], row['Column or Measure'], row['Expression'], row['Used In'])
            if row_tuple not in seen:
                unique_rows.append(row)
                seen.add(row_tuple)
        
        return unique_rows
    
    def write_metadata_to_csv(data, csv_output_path):
        """
        Writes the extracted Power BI metadata to a CSV file.

        Args:
            data (list): A list of dictionaries containing the extracted metadata.
            csv_output_path (str): The output path for the CSV file.
        """
        fieldnames = ['Report', 'Page', 'Table', 'Column or Measure', 'Expression', 'Used In']
        with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    # Extract metadata and write to CSV
    metadata = extract_power_bi_metadata_from_json_files(directory_path)
    write_metadata_to_csv(metadata, csv_output_path)
