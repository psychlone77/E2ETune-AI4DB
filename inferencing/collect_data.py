import argparse
import json
from pathlib import Path
import sys
import os

# Add parent directory to python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from classes.base_classes.Script_Config import ScriptConfig
from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.MySQL_Database import MySQLDatabase
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.DataCollectorOLAP import DataCollectorOLAP

def main():
    parser = argparse.ArgumentParser(description="Collect default data for a SQL file")
    parser.add_argument("sql_file", type=str, help="Path to the input SQL file")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="Path to overall config.yaml")
    parser.add_argument("--knob_config", type=str, default="knob_config/mysql64_knob_config_op.json", help="Path to knob config JSON file")
    parser.add_argument("--output_dir", type=str, default="inferencing/mysql/tpch", help="Output directory for JSON files")
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    if not sql_path.is_absolute():
        sql_path = Path(os.getcwd()) / sql_path

    if not sql_path.exists():
        print(f"Error: SQL file '{sql_path}' does not exist.")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = parent_dir / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    config_path = parent_dir / args.config
    script_config = ScriptConfig.from_yaml_file(str(config_path))

    # Initialize db
    try:
        # Simple heuristic to choose database implementation
        if script_config.database_config.port == 3306 or "mysql" in getattr(script_config.database_config, "data_path", "").lower():
            db = MySQLDatabase(script_config.database_config)
        else:
            db = PostgresSQLDatabase(script_config.database_config)
        db.connect()
    except Exception as e:
        print(f"Warning: Failed to connect to database. Make sure it's running. Details: {e}")
        # Could continue, but probably better to fail early if database connection fails
        sys.exit(1)

    # Load knob settings
    knob_config_path = parent_dir / args.knob_config
    if not knob_config_path.exists():
        print(f"Warning: Knob config file '{knob_config_path}' not found.")
        sys.exit(1)
        
    knob_settings = KnobSettingsSet.from_json_file(str(knob_config_path))

    # Use existing DataCollectorOLAP logic
    collector = DataCollectorOLAP(
        workload_path=sql_path,
        db=db,
        benchmark=script_config.benchmark_config.name,
        output_dir=output_dir,
        knob_settings_set=knob_settings,
        log_path=output_dir / "collector.log"
    )

    print(f"Starting data collection for {sql_path}")
    
    # 1. Internal Metrics
    print("Collecting internal metrics...")
    internal_metrics = collector._collect_internal_metrics()
    internal_metrics_dict = internal_metrics.__dict__ if hasattr(internal_metrics, '__dict__') else internal_metrics
        
    # 2. Query Plans
    print("Collecting query plans...")
    query_plans = collector._collect_query_plans()
        
    # 3. Workload Features
    print("Collecting workload features...")
    workload_features = collector._collect_workload_features()
    
    # Combine into a single dictionary
    output_data = {
        "internal_metrics": internal_metrics_dict,
        "query_plans": query_plans,
        "workload_features": workload_features
    }
    
    output_filename = f"{sql_path.stem}.json"
    output_file_path = output_dir / output_filename
    
    with open(output_file_path, "w") as f:
        json.dump(output_data, f, indent=4)
        
    print(f"\nData collection completed successfully. Saved to {output_file_path}")

if __name__ == "__main__":
    main()
