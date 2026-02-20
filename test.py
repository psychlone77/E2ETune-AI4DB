from classes.PostgreSQL_Database import PostgreSQLDatabase
from classes.Knob_Settings import KnobSettingsSet
from classes.Workload_Runner import BenchmarkTask
from classes.Script_Config import ScriptConfig

knob_config = KnobSettingsSet.from_json_file("knob_config/knob_config.json")

# Access knobs
for knob in knob_config.knobs:
    print(f"{knob.name}: {knob.default} (range: {knob.min}-{knob.max})")

# Get specific knob
shared_buffers = knob_config.get_knob("shared_buffers")
print(shared_buffers.describe)

# Get all defaults
defaults = knob_config.get_default_knob_settings()

print(defaults)


script_config = ScriptConfig.from_yaml_file("config/config.yaml")
print(script_config.database_config.host)  # localhost
print(script_config.tuning_config.method)  # HEBO

db = PostgreSQLDatabase(script_config.database_config)
wk = BenchmarkTask(
    workload_path="workloads/oltp_read_write/run.sh", knob_config=knob_config
)
db.run_workload(wk)
