from json2sql.core import json_to_sql

def test_placeholder():
    create, insert = json_to_sql("json2sql/examples/sample.json", "players")
    assert "CREATE" in create
    assert "INSERT" in insert
