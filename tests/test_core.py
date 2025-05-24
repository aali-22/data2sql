import pytest
from data2sql.core import (
    infer_type,
    infer_schema,
    load_data,
    generate_create_table,
    generate_insert_statements,
    process_data
)
import json
import sqlite3
from sqlalchemy import create_engine, text

def test_infer_type():
    """Test type inference for different values."""
    assert infer_type(None) == "TEXT"
    assert infer_type(True) == "BOOLEAN"
    assert infer_type(42) == "INTEGER"
    assert infer_type(3.14) == "REAL"
    assert infer_type("2024-03-15") == "DATE"
    assert infer_type("hello") == "TEXT"

def test_infer_schema(sample_json_data):
    """Test schema inference from JSON data."""
    schema = infer_schema(sample_json_data["players"])
    assert schema == {
        "name": "TEXT",
        "age": "INTEGER",
        "goals": "INTEGER",
        "xG": "REAL",
        "last_match": "DATE",
        "is_active": "BOOLEAN"
    }

def test_load_data_json(player_stats_json):
    """Test loading JSON data."""
    data = load_data(player_stats_json, "json")
    assert len(data) == 2
    assert data[0]["name"] == "Vinicius Jr"
    assert isinstance(data[0]["goals"], int)
    assert isinstance(data[0]["xG"], float)

def test_load_data_csv(match_stats_csv):
    """Test loading CSV data."""
    data = load_data(match_stats_csv, "csv")
    assert len(data) == 3
    assert data[0]["home_team"] == "Real Madrid"
    assert float(data[0]["possession_home"]) == 55.5

def test_generate_create_table():
    """Test CREATE TABLE statement generation."""
    schema = {
        "name": "TEXT",
        "age": "INTEGER",
        "xG": "REAL"
    }
    stmt = generate_create_table("players", schema)
    assert "CREATE TABLE IF NOT EXISTS players" in stmt
    assert "name TEXT" in stmt
    assert "age INTEGER" in stmt
    assert "xG REAL" in stmt

def test_generate_insert_statements():
    """Test INSERT statement generation."""
    schema = {"name": "TEXT", "age": "INTEGER"}
    data = [{"name": "Test", "age": 25}]
    stmts = generate_insert_statements("players", data, schema)
    assert len(stmts) == 1
    assert "INSERT INTO players (name, age) VALUES ('Test', 25);" in stmts

def test_process_data_to_sql_file(player_stats_json, temp_sql_file):
    """Test end-to-end processing to SQL file."""
    schema, create_stmt, insert_stmts = process_data(
        file_path=player_stats_json,
        table_name="players",
        format="json",
        output=temp_sql_file
    )
    
    # Check if file was created and contains SQL
    with open(temp_sql_file, 'r') as f:
        content = f.read()
        assert "CREATE TABLE IF NOT EXISTS players" in content
        assert "INSERT INTO players" in content

def test_process_data_to_database(player_stats_json, temp_db):
    """Test end-to-end processing to database."""
    schema, create_stmt, insert_stmts = process_data(
        file_path=player_stats_json,
        table_name="players",
        format="json",
        output=temp_db
    )
    
    # Verify data in database
    engine = create_engine(temp_db)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM players")).scalar()
        assert result == 2
        
        result = conn.execute(text("SELECT name, goals FROM players WHERE name = 'Vinicius Jr'")).first()
        assert result[0] == "Vinicius Jr"
        assert result[1] == 12
