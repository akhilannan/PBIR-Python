# PBIR Utils

`pbir_utils.py` is a Python module designed to facilitate the extraction and updating of metadata within Power BI PBIR files. This module provides several utility functions to help manage and manipulate PBIR metadata effectively.

## Features

- **Extract Metadata**: Retrieve key metadata informations from PBIR files.
- **Update Metadata**: Apply updates to metadata within PBIR files.

## Usage

### Export PBIR metadata information to a CSV file
```python
pbip_directory = r"C:\DEV\Power BI Report"
csv_path = r"C:\DEV\output.csv"
export_pbir_metadata_to_csv(pbip_directory, csv_path)
```

### Batch update on all PBIR components in a directory based on CSV Mapping
```python
pbip_directory = r"C:\DEV\Power BI Report"
csv_path = r"C:\DEV\Attribute_Mapping.csv"
batch_update_pbir_project(pbip_directory, csv_path)
```
