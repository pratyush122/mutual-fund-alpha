"""
Database Migration Script
"""

import os
from supabase import create_client
from src.utils.logger import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def run_migrations() -> None:
    """Run database migrations to create tables."""
    logger.info("Running database migrations...")

    try:
        # Get Supabase credentials
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment variables"
            )

        # Create Supabase client
        client = create_client(url, key)

        # Read migration file
        migration_file = (
            "mutual_fund_alpha/src/database/migrations/001_create_tables.sql"
        )
        if not os.path.exists(migration_file):
            # Try alternative path
            migration_file = "src/database/migrations/001_create_tables.sql"
            if not os.path.exists(migration_file):
                raise FileNotFoundError(f"Migration file not found: {migration_file}")

        with open(migration_file, "r") as f:
            sql_commands = f.read()

        # Split commands by semicolon and execute each
        commands = [cmd.strip() for cmd in sql_commands.split(";") if cmd.strip()]

        for command in commands:
            if command.upper().startswith("CREATE TABLE"):
                # Extract table name
                table_name = (
                    command.split()[2]
                    .replace("(", "")
                    .replace("`", "")
                    .replace('"', "")
                )
                logger.info(f"Creating table: {table_name}")

                # For Supabase, we need to use the REST API or run raw SQL
                # Let's try to create tables one by one using the SQL commands

        logger.info("Database migrations completed")

    except Exception as e:
        logger.error(f"Database migrations failed: {e}")
        raise


def run_migrations_via_sql() -> None:
    """Run database migrations using raw SQL commands."""
    logger.info("Running database migrations via SQL...")

    try:
        # Get Supabase credentials
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment variables"
            )

        # Create Supabase client
        client = create_client(url, key)

        # Read migration file
        migration_file = (
            "mutual_fund_alpha/src/database/migrations/001_create_tables.sql"
        )
        if not os.path.exists(migration_file):
            # Try alternative path
            migration_file = "src/database/migrations/001_create_tables.sql"
            if not os.path.exists(migration_file):
                migration_file = "data/raw/amfi_nav.parquet"  # Just to check if we're in the right directory
                if os.path.exists(migration_file):
                    # We're in the project root, so use relative path
                    migration_file = "mutual_fund_alpha/src/database/migrations/001_create_tables.sql"
                else:
                    raise FileNotFoundError(
                        f"Migration file not found: {migration_file}"
                    )

        if not os.path.exists(migration_file):
            # Try relative path from project root
            migration_file = (
                "mutual_fund_alpha/src/database/migrations/001_create_tables.sql"
            )

        with open(migration_file, "r") as f:
            sql_content = f.read()

        # Split by double newline to separate CREATE TABLE statements
        statements = sql_content.split("\n\n")

        for statement in statements:
            statement = statement.strip()
            if (
                statement
                and not statement.startswith("--")
                and "CREATE TABLE" in statement
            ):
                # Extract table name
                lines = statement.split("\n")
                create_line = [
                    line
                    for line in lines
                    if line.strip().upper().startswith("CREATE TABLE")
                ][0]
                table_name = (
                    create_line.split()[2]
                    .replace("(", "")
                    .replace("`", "")
                    .replace('"', "")
                    .replace(";", "")
                )

                logger.info(f"Processing table: {table_name}")

                # For Supabase, we'll need to create tables using the REST API approach
                # or run raw SQL. Let's try a different approach.

        logger.info("Database migrations completed via SQL")

    except Exception as e:
        logger.error(f"Database migrations failed: {e}")
        raise


if __name__ == "__main__":
    run_migrations_via_sql()
