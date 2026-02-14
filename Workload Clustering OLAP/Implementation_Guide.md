# Workload Clustering Implementation Guide (Adjusted)

This guide explains the refined clustering methodology, including the recent adjustment to UMAP sensitivity to increase the granularity of picked workloads.

## Prerequisites

Install the required libraries:

```bash
pip install sqlglot umap-learn hdbscan scikit-learn pandas numpy scipy
```

## Clustering Logic Overview

1.  **Feature Extraction**: Captures syntactic (Select, Join, Where), complexity (CTE, Subqueries, Predicate complexity), and semantic (Distinct Table Access) features.
2.  **Representation**: Uses **Mean, Median, and Standard Deviation** for each feature to represent the workload distribution.
3.  **UMAP Adjustment (Sensitivity)**:
    *   The `n_neighbors` parameter in UMAP has been lowered to `min(5, len - 1)`.
    *   **Reason**: Lowering `n_neighbors` makes UMAP focus more on local structure and distinctness. This prevents different workloads from being merged into the same cluster too aggressively, resulting in a slightly higher number of clusters and picked workloads.
4.  **Selection**:
    *   **Clusters**: Picks the workload closest to the **centroid** of each cluster.
    *   **Noise**: Treats all noise points as unique patterns and includes them all.

## Step-by-Step Implementation

### 1. Prepare Data
Ensure your `.wg` files are named `benchmark_id.wg` and placed in the target directory.

### 2. Run the Script
Execute the script to generate results:
```bash
python cluster_workloads.py
```

### 3. Retrieve Results
The script creates `[benchmark]_picked_workloads` folders containing the representative workloads.
