# Analysis Output Subfolder Structure

## Overview
All workload analysis files are now organized into workload-specific subfolders within `analysis_output/`.

## Directory Structure
```
analysis_output/
├── {workload_name}/
│   ├── {workload}_plans.txt
│   ├── {workload}_plans.json
│   ├── {workload}_features.txt
│   ├── {workload}_features.json
│   ├── {workload}_internal_metrics.json
│   ├── {workload}_plan_features.json
│   ├── {workload}_default_performance.json
│   ├── {workload}_default_results.csv
│   ├── {workload}_8_candidates.json
│   ├── {workload}_candidates_results.csv
│   ├── {workload}_candidates_analysis.txt
│   ├── {workload}_candidates_full_results.json
│   ├── {workload}_cost_model_results.csv
│   └── ...
```

## File Pipeline

### 1. default_runner.py
**Generates:** Initial workload analysis files
- Creates subfolder: `analysis_output/{workload_name}/`
- Saves all output files to the subfolder
- Files: plans, features, internal_metrics, default_performance, default_results.csv

**Usage:**
```bash
python default_runner.py job.sql
# Creates: analysis_output/job/
```

### 2. get_model_output.py
**Reads from:** Workload subfolder (with fallback to root)
- Looks in: `analysis_output/{workload_name}/`
- Falls back to: `analysis_output/` (for backward compatibility)
- Reads: internal_metrics, features, plans

**Generates:** LLM predictions
- Saves to: `analysis_output/{workload_name}/{workload}_8_candidates.json`

**Usage:**
```bash
python get_model_output.py job
# Reads from: analysis_output/job/
# Saves to: analysis_output/job/job_8_candidates.json
```

### 3. runDB_8_candidates.py
**Reads from:** Workload subfolder
- Looks in: `analysis_output/{workload_name}/`
- Reads: {workload}_8_candidates.json (from bin_edges.pkl conversion)

**Generates:** Database test results
- Saves to: `analysis_output/{workload_name}/`
- Files: candidates_results.csv, candidates_analysis.txt, candidates_full_results.json

**Usage:**
```bash
python runDB_8_candidates.py job.sql
# Reads from: analysis_output/job/job_8_candidates.json
# Saves to: analysis_output/job/
```

### 4. runCM_8_candidates.py
**Reads from:** Workload subfolder (with fallback to root)
- Looks in: `analysis_output/{workload_name}/`
- Falls back to: `analysis_output/` (for backward compatibility)
- Reads: internal_metrics, features, plan_features, candidates_full_results.json

**Generates:** Cost model predictions
- Saves to: `analysis_output/{workload_name}/{workload}_cost_model_results.csv`

**Usage:**
```bash
python runCM_8_candidates.py job
# Reads from: analysis_output/job/
# Saves to: analysis_output/job/job_cost_model_results.csv
```

## Complete Workflow Example

```bash
# Step 1: Analyze workload and test default config (4 runs, averaged)
python default_runner.py job.sql
# Output: analysis_output/job/{plans, features, metrics, default_results.csv}

# Step 2: Generate LLM predictions
python get_model_output.py job
# Output: analysis_output/job/job_8_candidates.json

# Step 3: Test candidates on real database
python runDB_8_candidates.py job.sql
# Output: analysis_output/job/{candidates_results.csv, analysis.txt, full_results.json}

# Step 4: Validate with cost model
python runCM_8_candidates.py job
# Output: analysis_output/job/job_cost_model_results.csv
```

## Benefits
1. **Organization:** All files for a workload are in one place
2. **Scalability:** Easy to manage multiple workloads simultaneously
3. **Clarity:** No filename conflicts between workloads
4. **Backward Compatibility:** Fallback to root directory when subfolders don't exist

## Updated Scripts
- ✅ default_runner.py - Creates and uses workload subfolder
- ✅ get_model_output.py - Reads from workload subfolder (with fallback)
- ✅ runDB_8_candidates.py - Reads from and saves to workload subfolder
- ✅ runCM_8_candidates.py - Reads from and saves to workload subfolder (with fallback)
