# E2ETune-AI4DB

E2ETune-AI4DB is an end-to-end database tuning and evaluation workspace built around an abstraction-first design. The core logic lives under `classes/`, where database runners, workload collectors, surrogate models, tuners, and workload feature extractors are separated behind shared base classes so the system can be extended to another database engine without rewriting the entire pipeline.

## Repository Shape

The most important parts of the codebase are:

* `main.py` for the main two-phase tuning workflow.
* `classes/` for the abstraction layer and engine-specific implementations.
* `inferencing/` for replaying and evaluating candidate knob configurations.
* `run_diverse_configs.py` for replaying precomputed diverse configurations.
* `config/config.yaml` for the primary runtime configuration.

## Requirements

Install Python dependencies with:

```bash
pip install -r requirements.txt
```

External components are still required for execution:

* PostgreSQL for OLAP/PG-based runs.
* MySQL for MySQL-backed runs.
* BenchBase for OLTP workload execution.

For BenchBase, build the PostgreSQL profile as usual:

```bash
git clone --depth 1 https://github.com/cmu-db/benchbase.git
cd benchbase
./mvnw clean package -P postgres
```

## Configuration

The current workflow uses only `config/config.yaml`.

That file controls:

* database connection details
* benchmark name, type, and workload directory
* tuning method and iteration settings
* surrogate model path
* knob definition file

The tuning code also depends on:

* `knob_config/` for knob ranges
* `representative_workloads_sampled.json` for Phase 1 workload sampling
* `10_diverse_configs_all.json` for the diverse-config replay flow

## Abstraction Layer

The repo is structured so the concrete database logic sits behind reusable interfaces:

* `classes/base_classes/Database.py` defines the shared database contract.
* `classes/base_classes/Data_Collector.py` defines the common collection flow.
* `classes/base_classes/Tuner.py` and `classes/HEBO_Tuner.py` separate tuning policy from database execution.
* `classes/base_classes/Workload_Runner.py` provides workload task definitions used across scripts.
* `classes/base_classes/Surrogate_Strategy.py` and `classes/Cost_Model.py` isolate surrogate behavior.

Engine-specific implementations such as `classes/PostgreSQL_Database.py`, `classes/MySQL_Database.py`, and `classes/BenchBase_Database.py` plug into the same flow. That makes it straightforward to add another database by implementing the same base contracts and wiring the new backend into the entry point.

## Main Tuning Flow

`main.py` is the primary entry point. It scans the benchmark workload folder and then runs:

* Phase 1: real execution on representative workloads sampled from `representative_workloads_sampled.json`
* Phase 2: surrogate-based tuning for the remaining workloads

It also resumes from existing performance records and can be forced into surrogate-only mode through the YAML tuning config.

Example:

```bash
python main.py --config config/config.yaml --dbengine postgresql --servername hetzner-4c-8t-32gb
```

Outputs are written to:

* `data/{dbengine}/{servername}/{benchmark}/{workload}/`
* `logs/tuning/`

## Inferencing Utilities

The `inferencing/` folder contains scripts for collecting workload data and replaying candidate configurations against a live database.

* `inferencing/collect_data.py` collects internal metrics, query plans, and workload features for one SQL file.
* `inferencing/evaluate_config.py` compares the default config against one chosen best config.
* `inferencing/evaluate_all_configs.py` replays a full JSON list of candidate knob buckets and tracks the best one.

Examples:

```bash
python inferencing/collect_data.py path/to/workload.sql
python inferencing/evaluate_config.py path/to/workload.sql
python inferencing/evaluate_all_configs.py --sql_file path/to/workload.sql --configs_json path/to/configs.json
```

## Diverse Config Replay

`run_diverse_configs.py` replays precomputed diverse knob settings from `10_diverse_configs_all.json`.

```bash
python run_diverse_configs.py --config config/config.yaml --dbengine mysql --servername hetzner-4c-8t-64gb
```

Use `--skip-configs` if you only want default data collection without running the replayed configurations.

## Fine-Tuning and Model Artifacts

The `llm_tuning/` folder contains notebook-based experiments for fine-tuning and adapter-based inference. Those notebooks assume the broader LLaMA-Factory / Hugging Face workflow described in the notebook cells themselves.

The published base model is available at:

* `springhxm/E2ETune`

## Output Folders

The most important generated folders are:

* `data/` for collected tuning artifacts
* `logs/` for tuning and workload logs
* `analysis_output/` for SQL analysis, candidates, and evaluation results
* `results/` for diverse-config replay outputs

## Notes

* OLTP workloads are executed through BenchBase configuration files in `oltp_workloads/`.
* OLAP workloads are stored under `olap_workloads/` and are typically replayed directly against the database.
* The repository mixes older helper scripts with newer refactored code, so check the script-specific help text before running any command.