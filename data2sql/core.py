import json
import pandas as pd
from typing import Dict, Tuple, List, Any, Optional
from datetime import datetime
from dateutil.parser import parse as parse_date
import sqlite3
from sqlalchemy import create_engine, text
import csv

def infer_type(value: Any) -> str:
    """Infer the SQL data type for a given value.

    Args:
        value (Any): The value to infer the type for. Can be None, bool, int, float, str, or other types.

    Returns:
        str: The inferred SQL type as a string. One of:
            - 'TEXT': For strings or None values
            - 'BOOLEAN': For boolean values
            - 'INTEGER': For integer values
            - 'REAL': For floating point values
            - 'DATE': For string values that can be parsed as dates
    """
    if value is None:
        return "TEXT"  # default to TEXT if value is null
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, str):
        try:
            parse_date(value) #check if the value is a date string
            return "DATE"
        except (ValueError, TypeError):
            return "TEXT"
    return "TEXT"

def infer_schema(data: List[Dict[str, Any]]) -> Dict[str, str]:
    """Infer SQL schema from a list of dictionaries.

    Args:
        data (List[Dict[str, Any]]): List of dictionaries where each dictionary represents a row of data.
            Each dictionary should have consistent keys representing column names.

    Returns:
        Dict[str, str]: A dictionary mapping column names to their inferred SQL types.
            Empty dictionary if input data is empty.
            For columns with all null values, defaults to 'TEXT'.
    """
    if not data:
        return {}
    
    schema = {}
    for key in data[0].keys(): # iterate over the keys in the first row of the data
        # Get non-null values for this key
        values = [row[key] for row in data if row.get(key) is not None] # get the values for the key in the data
        if not values:
            schema[key] = "TEXT"  # default to TEXT for all-null columns
            continue
            
        # Infer type from first non-null value
        schema[key] = infer_type(values[0])
    
    return schema

def load_data(file_path: str, format: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load data from a JSON or CSV file into a list of dictionaries.

    Args:
        file_path (str): Path to the input file.
        format (Optional[str], optional): Format of the input file ('json' or 'csv').
            If None, format is inferred from file extension. Defaults to None.

    Returns:
        List[Dict[str, Any]]: List of dictionaries where each dictionary represents a row of data.
            For JSON: Handles both single objects and arrays of objects.
            For CSV: Each row becomes a dictionary with column headers as keys.

    Raises:
        FileNotFoundError: If the input file doesn't exist.
        json.JSONDecodeError: If the JSON file is invalid.
        pd.errors.EmptyDataError: If the CSV file is empty.
    """
    if not format:
        format = 'json' if file_path.lower().endswith('.json') else 'csv'
    
    if format.lower() == 'json':
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Handle both single object and list of objects
            if isinstance(data, dict):
                # If it's a dict with a single key containing a list, use that list
                if len(data) == 1 and isinstance(next(iter(data.values())), list):
                    data = next(iter(data.values()))
                else:
                    data = [data]
            return data
    else:  # CSV
        df = pd.read_csv(file_path) # read the csv file into a dataframe
        return df.to_dict('records') # convert the dataframe to a list of dictionaries

def generate_create_table(table_name: str, schema: Dict[str, str]) -> str:
    """Generate a SQL CREATE TABLE statement from a schema definition.

    Args:
        table_name (str): Name of the table to create.
        schema (Dict[str, str]): Dictionary mapping column names to their SQL types.

    Returns:
        str: A SQL CREATE TABLE statement with IF NOT EXISTS clause.
            Format: CREATE TABLE IF NOT EXISTS table_name (
                       column1 type1,
                       column2 type2,
                       ...
                   );
    """
    fields = [f"{name} {type_}" for name, type_ in schema.items()] 
    field_list = ',\n    '.join(fields) # join the fields with a comma and a newline
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {field_list}\n);"   

def generate_insert_statements(table_name: str, data: List[Dict[str, Any]], schema: Dict[str, str]) -> List[str]:
    """Generate SQL INSERT statements for the data.

    Args:
        table_name (str): Name of the target table.
        data (List[Dict[str, Any]]): List of dictionaries containing the data to insert.
        schema (Dict[str, str]): Dictionary mapping column names to their SQL types.

    Returns:
        List[str]: List of SQL INSERT statements.
            - Null values are skipped in the INSERT statements
            - String values are properly escaped
            - Boolean values are converted to 1/0
            - Empty list if input data is empty

    Note:
        The function handles proper SQL escaping:
        - Strings and dates are quoted and escaped
        - Booleans are converted to 1/0
        - Numbers are converted to strings
    """
    if not data:
        return []
    
    statements = []
    for row in data:
        columns = []
        values = []
        for col, val in row.items():
            if val is not None:  # Skip NULL values
                columns.append(col)
                if schema[col] == "TEXT" or schema[col] == "DATE":
                    escaped_val = str(val).replace("'", "''")
                    values.append(f"'{escaped_val}'")
                elif schema[col] == "BOOLEAN":
                    values.append("1" if val else "0")
                else:
                    values.append(str(val))
        
        stmt = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
        statements.append(stmt)
    
    return statements

def write_to_database(create_stmt: str, insert_stmts: List[str], output_url: str):
    """Write SQL statements directly to a database.

    Args:
        create_stmt (str): The CREATE TABLE statement to execute.
        insert_stmts (List[str]): List of INSERT statements to execute.
        output_url (str): SQLAlchemy database URL (e.g., 'sqlite:///file.db' or 'postgresql://...').

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If there's an error connecting to or writing to the database.
    """
    engine = create_engine(output_url)
    with engine.connect() as conn:
        conn.execute(text(create_stmt))
        for stmt in insert_stmts:
            conn.execute(text(stmt))
        conn.commit()

def process_data(
    file_path: str,
    table_name: str,
    format: Optional[str] = None,
    output: Optional[str] = None,
    preview: bool = False) -> Tuple[Dict[str, str], str, List[str]]:
    """Main function to process data from file to SQL.

    Args:
        file_path (str): Path to the input file (JSON or CSV).
        table_name (str): Name of the target SQL table.
        format (Optional[str], optional): Format of the input file ('json' or 'csv').
            If None, format is inferred from file extension. Defaults to None.
        output (Optional[str], optional): Output destination.
            - If None: returns SQL statements without writing
            - If starts with 'sqlite:///' or 'postgresql://': writes directly to database
            - Otherwise: treats as file path and writes SQL to file
            Defaults to None.
        preview (bool, optional): Whether this is a preview run. Defaults to False.

    Returns:
        Tuple[Dict[str, str], str, List[str]]: A tuple containing:
            - schema: Dictionary mapping column names to their SQL types
            - create_stmt: The CREATE TABLE statement
            - insert_stmts: List of INSERT statements

    Raises:
        FileNotFoundError: If the input file doesn't exist
        ValueError: If the table name is invalid
        Various database errors if writing to database fails
    """
    # Load and infer schema
    data = load_data(file_path, format)
    schema = infer_schema(data)
    
    # Generate SQL
    create_stmt = generate_create_table(table_name, schema)
    insert_stmts = generate_insert_statements(table_name, data, schema)
    
    # Write to output if specified
    if output:
        if output.startswith(('sqlite:///', 'postgresql://')):
            write_to_database(create_stmt, insert_stmts, output)
        else: # if the output is not a database url, write the sql to a file
            with open(output, 'w') as f:
                f.write(create_stmt + '\n\n') 
                f.write('\n'.join(insert_stmts))
    
    return schema, create_stmt, insert_stmts
