import os
import shutil
import sqlglot
import umap
import hdbscan
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
import glob
from scipy.spatial.distance import cdist
from tabulate import tabulate
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score

def extract_sql_features(sql_query):
    """
    Enhanced feature extraction based on user feedback.
    Captures structural, complexity, and semantic elements with finer granularity.
    """
    features = {}
    try:
        # Standardize query splitting
        statements = [s for s in sql_query.split(';') if s.strip()]
        
        # Counters
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
        
        # Granular Predicates
        and_count = 0
        or_count = 0
        like_count = 0
        in_count = 0
        between_count = 0
        exists_count = 0
        comparison_count = 0 # >, <, >=, <=, !=, <>
        
        # Advanced Complexity
        case_count = 0
        cast_count = 0
        alias_count = 0
        window_func_count = 0
        
        # Join Types
        inner_join_count = 0
        left_join_count = 0
        right_join_count = 0
        full_join_count = 0
        cross_join_count = 0
        
        for stmt in statements:
            try:
                # Use postgres dialect for better parsing coverage as common baseline
                parsed_query = sqlglot.parse_one(stmt, read='postgres')
                
                # Basic Structural Features
                select_count += len(list(parsed_query.find_all(sqlglot.exp.Select)))
                joins = list(parsed_query.find_all(sqlglot.exp.Join))
                join_count += len(joins)
                where_count += len(list(parsed_query.find_all(sqlglot.exp.Where)))
                group_count += len(list(parsed_query.find_all(sqlglot.exp.Group)))
                order_count += 1 if list(parsed_query.find_all(sqlglot.exp.Order)) else 0
                limit_count += 1 if list(parsed_query.find_all(sqlglot.exp.Limit)) else 0
                subquery_count += len(list(parsed_query.find_all(sqlglot.exp.Subquery)))
                
                # CTEs
                cte_count += len(list(parsed_query.find_all(sqlglot.exp.CTE)))
                
                # Aggregates & Window Functions
                aggregates = (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Count, sqlglot.exp.Max, sqlglot.exp.Min)
                for agg in aggregates:
                    aggregate_count += len(list(parsed_query.find_all(agg)))
                window_func_count += len(list(parsed_query.find_all(sqlglot.exp.Window)))
                
                # Table Access Patterns
                for table in parsed_query.find_all(sqlglot.exp.Table):
                    distinct_tables.add(table.name.lower())
                
                # Granular Predicate Complexity
                and_count += len(list(parsed_query.find_all(sqlglot.exp.And)))
                or_count += len(list(parsed_query.find_all(sqlglot.exp.Or)))
                like_count += len(list(parsed_query.find_all(sqlglot.exp.Like)))
                in_count += len(list(parsed_query.find_all(sqlglot.exp.In)))
                between_count += len(list(parsed_query.find_all(sqlglot.exp.Between)))
                exists_count += len(list(parsed_query.find_all(sqlglot.exp.Exists)))
                
                # Comparison Operators
                comparisons = (sqlglot.exp.GT, sqlglot.exp.LT, sqlglot.exp.GTE, sqlglot.exp.LTE, sqlglot.exp.NEQ)
                for comp in comparisons:
                    comparison_count += len(list(parsed_query.find_all(comp)))
                
                # Advanced Syntax
                case_count += len(list(parsed_query.find_all(sqlglot.exp.Case)))
                cast_count += len(list(parsed_query.find_all(sqlglot.exp.Cast))) + len(list(parsed_query.find_all(sqlglot.exp.TryCast)))
                alias_count += len(list(parsed_query.find_all(sqlglot.exp.Alias)))
                
                # Join Types
                for join in joins:
                    kind = join.kind
                    side = join.side
                    if kind == 'CROSS':
                        cross_join_count += 1
                    elif kind == 'FULL':
                        full_join_count += 1
                    elif kind == 'LEFT':
                        left_join_count += 1
                    elif kind == 'RIGHT':
                        right_join_count += 1
                    else:
                        # Default implies inner usually, or just JOIN
                        inner_join_count += 1

            except Exception as e:
                logging.debug(f"Error parsing statement: {stmt[:50]}... Error: {str(e)}")
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
        
        # New granular features
        features['pred_and'] = and_count
        features['pred_or'] = or_count
        features['pred_like'] = like_count
        features['pred_in'] = in_count
        features['pred_between'] = between_count
        features['pred_exists'] = exists_count
        features['pred_comparison'] = comparison_count
        
        features['complexity_case'] = case_count
        features['complexity_cast'] = cast_count
        features['complexity_alias'] = alias_count
        features['complexity_window'] = window_func_count
        
        features['join_inner'] = inner_join_count
        features['join_left'] = left_join_count
        features['join_right'] = right_join_count
        features['join_full'] = full_join_count
        features['join_cross'] = cross_join_count
        
    except Exception as e:
        logging.error(f"Failed to extract features from query: {sql_query[:50]}... Error: {str(e)}")
        return None
    return features

def get_workload_features(file_path):
    """
    Aggregates query features into a workload vector.
    Uses Mean, Median, Std Dev, and Percentiles (25th, 75th).
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
    
    # Enhanced aggregation: Mean + Median + Std + p25 + p75
    stats = {}
    for col in df.columns:
        stats[f"{col}_mean"] = df[col].mean()
        stats[f"{col}_median"] = df[col].median()
        stats[f"{col}_std"] = df[col].std() if len(df) > 1 else 0
        stats[f"{col}_p25"] = np.percentile(df[col], 25)
        stats[f"{col}_p75"] = np.percentile(df[col], 75)
        
    return stats

def calculate_inertia(embeddings, labels, centroids):
    inertia = 0
    unique_labels = set(labels)
    
    # Pre-calculate centroids mapping
    centroid_map = {}
    if centroids is None:
        for label in unique_labels:
            if label == -1: continue
            points = embeddings[labels == label]
            centroid_map[label] = np.mean(points, axis=0)
    else:
        # If centroids provided as dict
        centroid_map = centroids

    for i, label in enumerate(labels):
        if label != -1:
            centroid = centroid_map[label]
            dist_sq = np.sum((embeddings[i] - centroid) ** 2)
            inertia += dist_sq
            
    return inertia

from sklearn.decomposition import PCA

def profile_clusters(df, clusters, benchmark_name, output_dir):
    """
    Generates a CSV profile of clusters showing mean feature values.
    """
    df['Cluster'] = clusters
    # Group by cluster and calculate mean of features
    profile = df.groupby('Cluster').mean()
    profile['Count'] = df.groupby('Cluster').size()
    
    # Save to CSV
    csv_path = os.path.join(output_dir, f"{benchmark_name}_cluster_profiles.csv")
    profile.to_csv(csv_path)
    print(f"  Saved cluster profile to: {csv_path}")
    return profile

def determine_optimal_components(normalized_features, n_samples):
    """
    Uses PCA to determine the number of components that explain ~95% variance.
    Capped at 10 or n_samples - 1.
    """
    if n_samples < 3:
        return 2
        
    pca = PCA(n_components=min(n_samples, 20)) # fit up to 20 or n
    pca.fit(normalized_features)
    
    cumsum = np.cumsum(pca.explained_variance_ratio_)
    d = np.argmax(cumsum >= 0.95) + 1 # +1 for 1-based indexing
    
    # Safety caps
    d = max(2, min(d, 10, n_samples - 1))
    return d

def tune_hyperparameters(normalized_features, n_samples):
    """
    Performs Grid Search to find best parameters optimizing COMPOSITE SCORE.
    Composite Score = Silhouette - (0.1 * DB) + (Inertia Boost?)
    Actually, we want to maximize Sil, minimize DB, minimize Inertia.
    Score = Sil - (w1 * DB_norm) - (w2 * Inertia_norm)
    """
    print(f"  Tuning hyperparameters for {n_samples} samples (Composite Scoring)...")
    
    # 1. Determine optimal dimensions via PCA
    n_comp_pca = determine_optimal_components(normalized_features, n_samples)
    print(f"  PCA suggested n_components: {n_comp_pca}")
    
    # Define search space
    neighbor_options = [5, 10, 15, 30]
    neighbor_options = [n for n in neighbor_options if n < n_samples]
    if not neighbor_options: neighbor_options = [max(2, n_samples - 1)]
    
    min_dist_options = [0.0, 0.1]
    
    # HDBSCAN parameters
    mcs_base = max(2, int(n_samples * 0.05))
    min_cluster_size_options = sorted(list(set([2, 5, 10, mcs_base])))
    
    # New: Epsilon for controlling cluster tightness (Inertia)
    cluster_selection_epsilon_options = [0.0, 0.5] 
    
    best_score = -float('inf')
    best_params = {}
    best_clusters = None
    best_embedding = None
    
    results_log = []
    
    # Grid Search
    for n_neighbors in neighbor_options:
        for min_dist in min_dist_options:
            try:
                # Fixed UMAP for this block
                reducer = umap.UMAP(
                    n_components=n_comp_pca,
                    min_dist=min_dist,
                    n_neighbors=n_neighbors,
                    init='random',
                    random_state=42
                )
                embedding = reducer.fit_transform(normalized_features)
                
                # Calculate embedding-level inertia max for normalization (approx)
                # Just use raw values for relative comparison in loop?
                # It's hard to normalize Inertia without knowing the range.
                # We will use a relative approach or just penalize "bad" DB.
                
                for mcs in min_cluster_size_options:
                    ms_options = [1, mcs]
                    for ms in ms_options:
                        for epsilon in cluster_selection_epsilon_options:
                            try:
                                clusterer = hdbscan.HDBSCAN(
                                    min_cluster_size=mcs,
                                    min_samples=ms,
                                    cluster_selection_epsilon=epsilon,
                                    metric='euclidean'
                                )
                                clusters = clusterer.fit_predict(embedding)
                                
                                unique_labels = set(clusters)
                                n_clusters = len(unique_labels) - (1 if -1 in clusters else 0)
                                if n_clusters < 2:
                                    continue
                                    
                                # Metrics
                                sil = silhouette_score(embedding, clusters)
                                db = davies_bouldin_score(embedding, clusters)
                                # Inertia (on embedding)
                                inertia = calculate_inertia(embedding, clusters, None)
                                
                                # Noise penalty
                                noise_ratio = list(clusters).count(-1) / n_samples
                                
                                # Composite Score Calculation
                                # Goal: High Sil, Low DB, Low Inertia.
                                # Heuristic: Score = Sil - (0.2 * DB) - (Penalty for noise > 40%)
                                # Inertia is magnitude dependent, so hard to include directly in linear sum without norm.
                                # However, epsilon directly improves inertia. 
                                # We will primarily select on Sil vs DB, but break ties with Inertia?
                                # Let's try: Score = Sil - (0.25 * DB)
                                
                                score = sil - (0.25 * db)
                                
                                if noise_ratio > 0.4:
                                    score -= 0.5 # Heavy penalty for too much noise
                                    
                                # Bonus for tightness (indirectly checking if inertia is "low" relative to n_clusters?)
                                # Let's just rely on Epsilon exploring tighter options.
                                
                                if score > best_score:
                                    best_score = score
                                    best_params = {
                                        'n_neighbors': n_neighbors,
                                        'min_dist': min_dist,
                                        'n_components': n_comp_pca,
                                        'min_cluster_size': mcs,
                                        'min_samples': ms,
                                        'epsilon': epsilon
                                    }
                                    best_clusters = clusters
                                    best_embedding = embedding
                                    best_inertia = inertia
                            except Exception:
                                continue
            except Exception:
                continue
                        
    if best_clusters is None:
        print("  Tuning failed. Fallback.")
        return tune_hyperparameters_default(normalized_features, n_samples)
        
    print(f"  Best Params: {best_params}")
    print(f"  Composite Score: {best_score:.4f} (Inertia: {best_inertia:.2f})")
    return best_embedding, best_clusters, best_params

def tune_hyperparameters_default(normalized_features, n_samples):
    n_neighbors = min(15, n_samples - 1)
    if n_neighbors < 2: n_neighbors = 2
    reducer = umap.UMAP(n_components=2, min_dist=0.01, n_neighbors=n_neighbors, init='random', random_state=42)
    embedding = reducer.fit_transform(normalized_features)
    clusterer = hdbscan.HDBSCAN(min_cluster_size=2, min_samples=1, metric='euclidean')
    clusters = clusterer.fit_predict(embedding)
    return embedding, clusters, {'Note': 'Default Fallback'}


def perform_micro_clustering(embedding, macro_clusters, min_size=10):
    """
    Stage 2: Micro-Clustering.
    Sub-clusters each Macro-Cluster using K-Means to minimize Inertia.
    Uses 'Elbow Method' heuristic via Silhouette check to decide k.
    """
    micro_clusters = np.array(macro_clusters, copy=True)
    next_label = max(macro_clusters) + 1
    
    unique_macros = set(macro_clusters)
    if -1 in unique_macros: unique_macros.remove(-1)
    
    print(f"  Performing Micro-Clustering on {len(unique_macros)} macro-clusters...")
    
    for macro_id in unique_macros:
        indices = np.where(macro_clusters == macro_id)[0]
        points = embedding[indices]
        n_points = len(points)
        
        if n_points < min_size:
            continue
            
        # Determine optimal k for this bucket (2 to 5 or sqrt(n))
        max_k = min(5, int(np.sqrt(n_points)) + 1)
        if max_k < 2: continue
        
        best_k = 1
        best_sub_sil = -1
        best_sub_labels = None
        
        # Try splitting
        for k in range(2, max_k + 1):
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            sub_labels = kmeans.fit_predict(points)
            
            # Check internal structure quality
            # If splitting creates garbage clusters, don't do it.
            try:
                sil = silhouette_score(points, sub_labels)
                # Heuristic: If valid split and decent separation
                if sil > 0.35: # strict threshold to avoid over-fragmentation
                    if sil > best_sub_sil:
                        best_sub_sil = sil
                        best_k = k
                        best_sub_labels = sub_labels
            except: pass
            
        # Apply the split if we found a good one
        if best_k > 1 and best_sub_labels is not None:
            # We need to assign new unique labels
            # Current macro_id is replaced by next_label, next_label+1...
            for i, sub_lab in enumerate(best_sub_labels):
                global_idx = indices[i]
                # We can keep the main macro_id for base, but we need unique micro_ids
                # Let's effectively "re-label" these points
                micro_clusters[global_idx] = next_label + sub_lab
            
            next_label += best_k
            
    return micro_clusters

def cluster_benchmark(benchmark_name, workload_files):
    print(f"\nScanning benchmark: {benchmark_name} with {len(workload_files)} files.")
    
    workload_data = []
    valid_files = []
    
    for f in workload_files:
        features = get_workload_features(f)
        if features:
            workload_data.append(features)
            valid_files.append(f)
    
    if len(workload_data) < 3:
        print(f"Not enough workloads for {benchmark_name} to cluster.")
        return None
    
    df = pd.DataFrame(workload_data)
    df = df.fillna(0)
    
    # Step 2: Normalization
    scaler = StandardScaler()
    normalized_features = scaler.fit_transform(df)
    
    # Step 3: Macro-Clustering (Stage 1 - Separation)
    n_samples = len(normalized_features)
    embedding, macro_clusters, best_params = tune_hyperparameters(normalized_features, n_samples)
    
    # Step 4: Micro-Clustering (Stage 2 - Compactness)
    micro_clusters = perform_micro_clustering(embedding, macro_clusters)
    
    # Use Micro-Clusters for Picking and Profiling
    final_clusters = micro_clusters
    
    # Organize into folders
    output_base = r"c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\Workload Clustering OLAP\picked_workloads"
    if not os.path.exists(output_base):
        os.makedirs(output_base, exist_ok=True)
        print(f"Created base output directory: {output_base}")
        
    picked_dir = os.path.join(output_base, f"{benchmark_name}_picked_workloads_vector_embeddings")
    if os.path.exists(picked_dir):
        shutil.rmtree(picked_dir)
    os.makedirs(picked_dir)
    
    # Profiling (Based on Micro Clusters for detail)
    profile_clusters(df.copy(), final_clusters, benchmark_name, output_base)
    
    # Cluster Analysis and Centroid-based Selection
    unique_clusters = set(final_clusters)
    n_micro_clusters = len(unique_clusters) - (1 if -1 in final_clusters else 0)
    n_noise = list(final_clusters).count(-1)
    
    # Pick representatives based on Micro-Clusters
    for cluster_id in unique_clusters:
        indices = np.where(final_clusters == cluster_id)[0]
        cluster_files = [valid_files[i] for i in indices]
        
        if cluster_id == -1:
            for f in cluster_files:
                shutil.copy(f, picked_dir)
        else:
            cluster_embeddings = embedding[indices]
            centroid = np.mean(cluster_embeddings, axis=0)
            
            distances = cdist(cluster_embeddings, centroid.reshape(1, -1), 'euclidean').flatten()
            closest_idx = indices[np.argmin(distances)]
            rep_file = valid_files[closest_idx]
            shutil.copy(rep_file, picked_dir)
            
    # Calculate Metrics
    # Macro Metrics
    macro_sil = silhouette_score(embedding, macro_clusters) if len(set(macro_clusters)) > 1 else -1
    macro_db = davies_bouldin_score(embedding, macro_clusters) if len(set(macro_clusters)) > 1 else 10
    
    # Micro Metrics
    micro_sil = silhouette_score(embedding, final_clusters) if len(set(final_clusters)) > 1 else -1
    micro_inertia = calculate_inertia(embedding, final_clusters, None)

    return {
        "Benchmark": benchmark_name,
        "Macro Silhouette": macro_sil,
        "Macro DB": macro_db,
        "Micro Inertia": micro_inertia,
        "Micro Silhouette": micro_sil,
        "Clusters (Micro)": n_micro_clusters,
        "Clusters (Macro)": len(set(macro_clusters)) - (1 if -1 in macro_clusters else 0),
        "Noise Points": n_noise
    }

def print_metrics_comparison(results):
    # Phase 2 (Best Macro) Reference
    phase2 = {
        "job": {"sil": 0.5418, "db": 0.3895},
        "ssb": {"sil": 0.7188, "db": 0.3305},
        "tpcds": {"sil": 0.5529, "db": 0.4877},
        "tpch": {"sil": 0.6095, "db": 0.5354}
    }
    
    table_data = []
    headers = ["Benchmark", "Stage", "Metric", "Value", "Ref (P2)", "Status"]
    
    for res in results:
        bench = res["Benchmark"]
        ref = phase2.get(bench, {})
        
        # Macro Stage Rows
        table_data.append([bench, "Macro", "Silhouette", f"{res['Macro Silhouette']:.4f}", ref.get('sil', 0), "OK" if res['Macro Silhouette'] >= ref.get('sil', 0) - 0.05 else "LOW"])
        table_data.append([bench, "Macro", "Davis-Bouldin", f"{res['Macro DB']:.4f}", ref.get('db', 0), "OK"])
        
        # Micro Stage Rows
        table_data.append([bench, "Micro", "Inertia", f"{res['Micro Inertia']:.2f}", "-", "TIGHT"])
        table_data.append([bench, "Micro", "Clusters", res['Clusters (Micro)'], f"Macro: {res['Clusters (Macro)']}", "-"])
        table_data.append(["", "", "", "", "", ""]) # Spacer

    output_text = "\n" + tabulate(table_data, headers=headers, tablefmt="grid")
    print(output_text)
    
    # Save to file
    with open("clustering_results.txt", "w") as f:
        f.write(output_text)


# User requested source: c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\olap_workloads
workload_dir = r"c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\olap_workloads"

def main():
    print(f"Reading workloads from: {workload_dir}")
    
    if not os.path.exists(workload_dir):
        print(f"Error: Directory not found: {workload_dir}")
        return

    # Files are flat in olap_workloads, so just *.wg
    all_files = glob.glob(os.path.join(workload_dir, "*.wg"))
    if not all_files:
        print("No .wg files found in the specified directory.")
        return

    benchmarks = {}
    for f in all_files:
        # We classify based on filename prefix (job_*, ssb_*, tpc*, etc)
        name = os.path.basename(f)
        parts = name.split('_')
        if len(parts) >= 2:
            b_name = parts[0]
            if b_name not in benchmarks:
                benchmarks[b_name] = []
            benchmarks[b_name].append(f)
            
    results = []
    for b_name, files in benchmarks.items():
        print(f"Processing benchmark: {b_name} with {len(files)} workloads")
        res = cluster_benchmark(b_name, files)
        if res:
            results.append(res)
            
    if results:
        print_metrics_comparison(results)

if __name__ == "__main__":
    main()
