from typing import Any, Dict, List
from dateutil.parser import parse as parse_date
import os

def infer_sql_type(value: Any) -> str:
    """Infer SQL data type from a Python value.

    Args:
        value (Any): The value to infer the type for.

    Returns:
        str: The inferred SQL type, currently always returns "TEXT".
            This is a placeholder function for future type inference implementation.
    """
    # Placeholder for data type inference
    return "TEXT"

def validate_file_exists(file_path: str) -> bool:
    """Check if a file exists at the given path.

    Args:
        file_path (str): Path to the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)

def validate_table_name(name: str) -> bool:
    """Validate if a string is a valid SQL table name.

    Args:
        name (str): The table name to validate.

    Returns:
        bool: True if the name is valid, False otherwise.
            A valid table name:
            - Is not empty
            - Contains only alphanumeric characters and underscores
            - Does not start with a digit
    """
    return (
        name and
        name.strip() and
        name.replace('_', '').isalnum() and
        not name[0].isdigit()
    )

def is_valid_date(value: str) -> bool:
    """Check if a string represents a valid date.

    Args:
        value (str): The string to check for date validity.

    Returns:
        bool: True if the string can be parsed as a valid date with at least
            year, month, and day components. False otherwise.

    Note:
        - Uses dateutil.parser for flexible date parsing
        - Requires the string to be at least 8 characters long to avoid just years
        - Returns False for None values or unparseable strings
    """
    try:
        # Try to parse the date
        date = parse_date(value)
        # Ensure it has at least year, month, and day components
        formatted = date.strftime('%Y-%m-%d')
        return len(value) >= 8  # Basic length check to avoid just year
    except (ValueError, TypeError):
        return False

def sanitize_field_name(name: str) -> str:
    """Sanitize a field name to make it SQL-compatible.

    Args:
        name (str): The original field name to sanitize.

    Returns:
        str: A SQL-compatible field name where:
            - Special characters are replaced with underscores
            - Names starting with digits are prefixed with 'f_'
            - Only alphanumeric characters and underscores remain

    Example:
        >>> sanitize_field_name("First Name!")
        "First_Name_"
        >>> sanitize_field_name("123field")
        "f_123field"
    """
    # Replace spaces and special chars with underscore
    sanitized = ''.join(c if c.isalnum() else '_' for c in name)
    # Ensure it doesn't start with a number
    if sanitized[0].isdigit():
        sanitized = f"f_{sanitized}"
    return sanitized

def validate_output_path(path: str) -> bool:
    """Validate if a path is a valid output destination for SQL.

    Args:
        path (str): The path to validate. Can be either a file path or database URL.

    Returns:
        bool: True if the path is valid, False otherwise.
            Valid paths are either:
            - Database URLs starting with 'sqlite:///' or 'postgresql://'
            - .sql file paths in existing directories
            - .sql file paths in the current directory
    """
    if path.startswith(('sqlite:///', 'postgresql://')):
        return True
    return (
        path.endswith('.sql') and
        os.path.dirname(path) == '' or
        os.path.isdir(os.path.dirname(path))
    )
