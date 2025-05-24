# data2sql

A flexible CLI tool to convert football data (JSON/CSV) into SQL tables.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Basic usage:
```bash
python main.py convert --file input.json --table player_stats
```

Options:
- `--file`: Input JSON or CSV file (required)
- `--format`: Input format (json/csv, auto-detected if not specified)
- `--table`: Name of the SQL table to create (required)
- `--output`: Output file (.sql) or database URL (e.g. sqlite:///out.db)
- `--preview`: Preview the inferred schema
- `--interactive`: Interactively confirm or modify field types

Examples:

1. Preview schema before conversion:
```bash
python main.py convert --file stats.json --table player_stats --preview
```

2. Write to SQLite database:
```bash
python main.py convert --file stats.csv --table match_stats --output sqlite:///football.db
```

3. Interactive schema modification:
```bash
python main.py convert --file player.json --table players --interactive
```

## Features

- Automatic schema inference from JSON/CSV data
- Support for basic SQL types (TEXT, INTEGER, REAL, DATE, BOOLEAN)
- Interactive schema modification
- Output to .sql file or direct database insertion
- Cross-platform support

## Development

The codebase is structured as follows:
- `cli.py`: Command-line interface using Click
- `core.py`: Core data processing and SQL generation
- `utils.py`: Utility functions for validation and type inference

## License

MIT

```bash
python main.py --input json2sql/examples/sample.json --table players
