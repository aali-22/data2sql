import click
from typing import Optional
from .core import (
    process_data,
    generate_create_table,
    generate_insert_statements,
    load_data,
    write_to_database
)
from .utils import validate_table_name, validate_file_exists
import sys
from pprint import pprint

@click.group()
def cli():
    """Convert JSON/CSV  data to SQL tables."""
    pass

@cli.command()
@click.option('--file', required=True, help='Input JSON or CSV file')
@click.option('--format', type=click.Choice(['json', 'csv']), help='Input file format (auto-detected if not specified)')
@click.option('--table', required=True, help='Name of the SQL table to create')
@click.option('--output', help='Output file (.sql) or database URL (sqlite:///file.db)')
@click.option('--preview', is_flag=True, help='Preview the inferred schema')
@click.option('--interactive', is_flag=True, help='Interactively confirm or modify schema')
def convert(
    file: str,
    table: str,
    format: Optional[str],
    output: Optional[str],
    preview: bool,
    interactive: bool
):
    """Convert a JSON or CSV file to SQL statements."""
    try:
        # Validate inputs
        if not validate_file_exists(file):
            raise click.ClickException(f"Input file not found: {file}")
        
        if not validate_table_name(table):
            raise click.ClickException(
                f"Invalid table name: {table}. Table name must start with a letter and contain only letters, numbers, and underscores."
            )
        
        # Load data first
        data = load_data(file, format)
        
        # Process the data
        schema, create_stmt, insert_stmts = process_data(
            file_path=file,
            table_name=table,
            format=format,
            output=None if interactive else output,  # Don't write to output yet if in interactive mode
            preview=preview
        )
        
        # Preview mode
        if preview:
            click.echo("\nInferred Schema:")
            for field, type_ in schema.items():
                click.echo(f"{field}: {type_}")
            click.echo("\nPreview of CREATE TABLE statement:")
            click.echo(create_stmt)
            if not interactive:
                return

        # Interactive mode
        if interactive:
            click.echo("\nPlease confirm or modify the schema:")
            new_schema = {}
            for field, type_ in schema.items():
                new_type = click.prompt(
                    f"Field '{field}' (detected as {type_})",
                    type=click.Choice(['TEXT', 'INTEGER', 'REAL', 'DATE', 'BOOLEAN']),
                    default=type_
                )
                new_schema[field] = new_type
            
            # Regenerate SQL with new schema
            schema = new_schema
            create_stmt = generate_create_table(table, schema)
            insert_stmts = generate_insert_statements(table, data, schema)
            
            # Write to output if specified
            if output:
                if output.startswith(('sqlite:///', 'postgresql://')):
                    write_to_database(create_stmt, insert_stmts, output)
                else:
                    with open(output, 'w') as f:
                        f.write(create_stmt + '\n\n')
                        f.write('\n'.join(insert_stmts))

        # Output results
        if output:
            click.echo(f"\nSQL has been written to: {output}")
        else:
            click.echo("\nGenerated SQL:")
            click.echo(create_stmt)
            click.echo("\nFirst few INSERT statements:")
            for stmt in insert_stmts[:3]:
                click.echo(stmt)
            if len(insert_stmts) > 3:
                click.echo("... and more INSERT statements")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def main():
    cli()
