# reset databases knobs 
import psycopg2
from typing import Dict, Any

def reset_db_knobs() -> None:
    """
    Reset the database knobs to their default values.
    """
    conn = None
    args: Dict[str, Any] = {
        'database_config': {
            'host': 'localhost',
            'database': 'imdb',
            'user': 'azureuser',
            'password': 'databasefyp'
        }
    }
    try:
        conn = psycopg2.connect(
            host=args['database_config']['host'],
            database=args['database_config']['database'],
            user=args['database_config']['user'],
            password=args['database_config']['password']
        )
        # ALTER SYSTEM cannot run inside a transaction block; enable autocommit
        conn.autocommit = True
        cur = conn.cursor()

        # Simplest and safest: clear all overrides from postgresql.auto.conf
        # This resets parameters back to their default/boot values.
        cur.execute("ALTER SYSTEM RESET ALL;")

        # Reload the configuration to apply changes immediately
        cur.execute("SELECT pg_reload_conf();")
        cur.close()
        print("Database knobs have been reset to default values.")

    except Exception as e:
        print(f"An error occurred while resetting database knobs: {e}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    reset_db_knobs()