import os
import shutil
import sqlglot
import umap
import hdbscan
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import glob
from scipy.spatial.distance import cdist

def extract_sql_features(sql_query):
    """
    Enhanced feature extraction based on user feedback.
    Captures structural, complexity, and semantic elements.
    """
    features = {}
    try:
        # Standardize query splitting
        statements = [s for s in sql_query.split(';') if s.strip()]
        
        select_count = 0
        join_count = 0
        where_count = 0
        group_count = 0
        order_count = 0
        limit_count = 0
        subquery_count = 0
        aggregate_count = 0
        cte_count = 0
        distinct_tables = set()
        predicate_complexity = 0 # count AND/OR/LIKE
        
        for stmt in statements:
            try:
                # Use a generic dialect or attempt to detect
                parsed_query = sqlglot.parse_one(stmt)
                
                # Basic Structural Features
                select_count += len(list(parsed_query.find_all(sqlglot.exp.Select)))
                join_count += len(list(parsed_query.find_all(sqlglot.exp.Join)))
                where_count += len(list(parsed_query.find_all(sqlglot.exp.Where)))
                group_count += len(list(parsed_query.find_all(sqlglot.exp.Group)))
                order_count += 1 if list(parsed_query.find_all(sqlglot.exp.Order)) else 0
                limit_count += 1 if list(parsed_query.find_all(sqlglot.exp.Limit)) else 0
                subquery_count += len(list(parsed_query.find_all(sqlglot.exp.Subquery)))
                
                # CTEs
                cte_count += len(list(parsed_query.find_all(sqlglot.exp.CTE)))
                
                # Aggregates
                aggregates = (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Count, sqlglot.exp.Max, sqlglot.exp.Min)
                for agg in aggregates:
                    aggregate_count += len(list(parsed_query.find_all(agg)))
                
                # Table Access Patterns
                for table in parsed_query.find_all(sqlglot.exp.Table):
                    distinct_tables.add(table.name.lower())
                
                # Predicate Complexity (AND, OR, LIKE)
                predicate_complexity += len(list(parsed_query.find_all(sqlglot.exp.And)))
                predicate_complexity += len(list(parsed_query.find_all(sqlglot.exp.Or)))
                predicate_complexity += len(list(parsed_query.find_all(sqlglot.exp.Like)))
                
            except Exception:
                continue

        features['selects'] = select_count
        features['joins'] = join_count
        features['wheres'] = where_count
        features['groups'] = group_count
        features['orders'] = order_count
        features['limits'] = limit_count
        features['subqueries'] = subquery_count
        features['aggregates'] = aggregate_count
        features['ctes'] = cte_count
        features['distinct_tables'] = len(distinct_tables)
        features['predicate_complexity'] = predicate_complexity
        
    except Exception as e:
        return None
    return features

def get_workload_features(file_path):
    """
    Aggregates query features into a workload vector.
    Uses Mean, Median, and Standard Deviation to capture distribution.
    """
    with open(file_path, 'r') as f:
        queries = f.readlines()
    
    all_query_features = []
    for q in queries:
        if q.strip():
            feat = extract_sql_features(q)
            if feat:
                all_query_features.append(feat)
    
    if not all_query_features:
        return None
    
    df = pd.DataFrame(all_query_features)
    
    # Refined aggregation: Mean + Median + Std Dev
    stats = {}
    for col in df.columns:
        stats[f"{col}_mean"] = df[col].mean()
        stats[f"{col}_median"] = df[col].median()
        stats[f"{col}_std"] = df[col].std() if len(df) > 1 else 0
        
    return stats

def cluster_benchmark(benchmark_name, workload_files):
    print(f"\nProcessing benchmark: {benchmark_name}")
    
    workload_data = []
    valid_files = []
    
    for f in workload_files:
        features = get_workload_features(f)
        if features:
            workload_data.append(features)
            valid_files.append(f)
    
    if len(workload_data) < 3:
        print(f"Not enough workloads for {benchmark_name} to cluster.")
        return
    
    df = pd.DataFrame(workload_data)
    # Fill NaN std dev with 0
    df = df.fillna(0)
    
    # Step 2: Normalization
    scaler = StandardScaler()
    normalized_features = scaler.fit_transform(df)
    
    # Step 3: UMAP
    # User adjustment: Lower neighbors preserves local distinctness better
    n_neighbors = min(5, len(normalized_features) - 1)
    if n_neighbors < 2: n_neighbors = 2
    
    n_comp = min(16, len(normalized_features) - 1)
    if n_comp < 2: n_comp = 2

    reducer = umap.UMAP(
        n_components=n_comp,
        min_dist=0.01, 
        n_neighbors=n_neighbors, 
        random_state=42
    )
    embedding = reducer.fit_transform(normalized_features)
    
    # Step 4: HDBSCAN
    # Dynamic parameter tuning
    min_cluster_size = max(2, int(len(embedding) * 0.05)) if len(embedding) > 20 else 2
    
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, 
        min_samples=1, 
        metric='euclidean'
    )
    clusters = clusterer.fit_predict(embedding)
    
    # Organize into folders
    benchmark_dir = os.path.dirname(workload_files[0])
    picked_dir = os.path.join(benchmark_dir, f"{benchmark_name}_picked_workloads")
    if os.path.exists(picked_dir):
        shutil.rmtree(picked_dir)
    os.makedirs(picked_dir)
    
    # Cluster Analysis and Centroid-based Selection
    unique_clusters = set(clusters)
    print(f"Found {len(unique_clusters) - (1 if -1 in clusters else 0)} clusters and {list(clusters).count(-1)} noise points.")
    
    for cluster_id in unique_clusters:
        indices = np.where(clusters == cluster_id)[0]
        cluster_files = [valid_files[i] for i in indices]
        
        if cluster_id == -1:
            # Noise points: treat each as unique and pick all
            for f in cluster_files:
                shutil.copy(f, picked_dir)
        else:
            # Centroid-based selection: pick file closest to cluster centroid in embedding space
            cluster_embeddings = embedding[indices]
            centroid = np.mean(cluster_embeddings, axis=0).reshape(1, -1)
            distances = cdist(cluster_embeddings, centroid, 'euclidean').flatten()
            closest_idx = indices[np.argmin(distances)]
            rep_file = valid_files[closest_idx]
            shutil.copy(rep_file, picked_dir)
            
    print(f"Picked workloads saved to: {picked_dir}")

def main():
    workload_dir = "/home/ubuntu/workload_clustering/workloads"
    all_files = glob.glob(os.path.join(workload_dir, "*.wg"))
    
    benchmarks = {}
    for f in all_files:
        name = os.path.basename(f)
        parts = name.split('_')
        if len(parts) >= 2:
            b_name = parts[0]
            if b_name not in benchmarks:
                benchmarks[b_name] = []
            benchmarks[b_name].append(f)
    
    for b_name, files in benchmarks.items():
        cluster_benchmark(b_name, files)

if __name__ == "__main__":
    main()
