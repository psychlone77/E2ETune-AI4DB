import os
import shutil
import sqlglot
import umap
import hdbscan
import pandas as pd
import numpy as np
import logging
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.decomposition import PCA
from scipy.spatial.distance import cdist
from gensim.models import FastText
import glob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
warnings.filterwarnings('ignore')

# --- 1. COMPREHENSIVE FEATURE EXTRACTION (MAINTAINED) ---
def extract_sql_features(sql_query):
    features = {}
    try:
        statements = [s for s in sql_query.split(';') if s.strip()]
        counts = {
            'selects': 0, 'joins': 0, 'wheres': 0, 'groups': 0, 'orders': 0, 
            'limits': 0, 'subqueries': 0, 'aggregates': 0, 'ctes': 0, 
            'distinct_tables': set(), 
            'pred_and': 0, 'pred_or': 0, 'pred_like': 0, 'pred_in': 0, 
            'pred_between': 0, 'pred_exists': 0, 'pred_comparison': 0,
            'complexity_case': 0, 'complexity_cast': 0, 'complexity_alias': 0, 'complexity_window': 0,
            'join_inner': 0, 'join_left': 0, 'join_right': 0, 'join_full': 0, 'join_cross': 0
        }
        for stmt in statements:
            try:
                parsed = sqlglot.parse_one(stmt, read='postgres')
                counts['selects'] += len(list(parsed.find_all(sqlglot.exp.Select)))
                joins = list(parsed.find_all(sqlglot.exp.Join))
                counts['joins'] += len(joins)
                counts['wheres'] += len(list(parsed.find_all(sqlglot.exp.Where)))
                counts['groups'] += len(list(parsed.find_all(sqlglot.exp.Group)))
                counts['orders'] += 1 if list(parsed.find_all(sqlglot.exp.Order)) else 0
                counts['limits'] += 1 if list(parsed.find_all(sqlglot.exp.Limit)) else 0
                counts['subqueries'] += len(list(parsed.find_all(sqlglot.exp.Subquery)))
                counts['ctes'] += len(list(parsed.find_all(sqlglot.exp.CTE)))
                for agg in (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Count, sqlglot.exp.Max, sqlglot.exp.Min):
                    counts['aggregates'] += len(list(parsed.find_all(agg)))
                counts['complexity_window'] += len(list(parsed.find_all(sqlglot.exp.Window)))
                for table in parsed.find_all(sqlglot.exp.Table):
                    counts['distinct_tables'].add(table.name.lower())
                counts['pred_and'] += len(list(parsed.find_all(sqlglot.exp.And)))
                counts['pred_or'] += len(list(parsed.find_all(sqlglot.exp.Or)))
                counts['pred_like'] += len(list(parsed.find_all(sqlglot.exp.Like)))
                counts['pred_in'] += len(list(parsed.find_all(sqlglot.exp.In)))
                counts['pred_between'] += len(list(parsed.find_all(sqlglot.exp.Between)))
                counts['pred_exists'] += len(list(parsed.find_all(sqlglot.exp.Exists)))
                for comp in (sqlglot.exp.GT, sqlglot.exp.LT, sqlglot.exp.GTE, sqlglot.exp.LTE, sqlglot.exp.NEQ):
                    counts['pred_comparison'] += len(list(parsed.find_all(comp)))
                counts['complexity_case'] += len(list(parsed.find_all(sqlglot.exp.Case)))
                counts['complexity_cast'] += len(list(parsed.find_all(sqlglot.exp.Cast))) + len(list(parsed.find_all(sqlglot.exp.TryCast)))
                counts['complexity_alias'] += len(list(parsed.find_all(sqlglot.exp.Alias)))
                for j in joins:
                    kind = j.kind
                    if kind == 'CROSS': counts['join_cross'] += 1
                    elif kind == 'FULL': counts['join_full'] += 1
                    elif kind == 'LEFT': counts['join_left'] += 1
                    elif kind == 'RIGHT': counts['join_right'] += 1
                    else: counts['join_inner'] += 1
            except: continue
        features = {k: (len(v) if isinstance(v, set) else v) for k, v in counts.items()}
    except: return None
    return features

def get_vector_workload_features(file_path):
    with open(file_path, 'r') as f:
        queries = f.readlines()
    all_feats = [extract_sql_features(q) for q in queries if q.strip()]
    all_feats = [f for f in all_feats if f]
    if not all_feats: return None
    df = pd.DataFrame(all_feats)
    stats = {}
    for col in df.columns:
        stats[f"{col}_mean"] = df[col].mean()
        stats[f"{col}_median"] = df[col].median()
        stats[f"{col}_std"] = df[col].std() if len(df) > 1 else 0
        stats[f"{col}_p25"] = np.percentile(df[col], 25)
        stats[f"{col}_p75"] = np.percentile(df[col], 75)
    return stats

# --- 2. WORD EMBEDDINGS ---
def tokenize_query(query):
    try:
        parsed = sqlglot.parse_one(query, read='postgres')
        return [str(token).lower() for token in parsed.flatten() if token]
    except:
        return query.lower().replace(',', ' , ').replace('(', ' ( ').replace(')', ' ) ').split()

def get_word_workload_embedding(file_path, model):
    try:
        with open(file_path, 'r') as f:
            queries = f.readlines()
        q_embs = []
        for q in queries:
            tokens = tokenize_query(q.strip())
            vectors = [model.wv[t] for t in tokens if t in model.wv]
            if vectors: q_embs.append(np.mean(vectors, axis=0))
        if not q_embs: return None
        q_embs = np.array(q_embs)
        return np.concatenate([
            np.mean(q_embs, axis=0), 
            np.std(q_embs, axis=0),
            np.percentile(q_embs, 25, axis=0),
            np.percentile(q_embs, 75, axis=0)
        ])
    except: return None

# --- 3. EVALUATION METRICS ---
def calculate_inertia(embedding, labels):
    inertia = 0
    unique_labels = set(labels)
    for label in unique_labels:
        if label == -1: continue
        points = embedding[labels == label]
        centroid = np.mean(points, axis=0)
        inertia += np.sum(np.sum((points - centroid) ** 2, axis=1))
    return inertia

def evaluate_clustering(embedding, labels):
    unique_labels = set(labels)
    n_clusters = len(unique_labels) - (1 if -1 in labels else 0)
    inertia = calculate_inertia(embedding, labels)
    
    if n_clusters < 2:
        return {"Sil": -1.0, "DB": 10.0, "CH": 0.0, "Inertia": inertia, "Noise": list(labels).count(-1)}
    
    mask = labels != -1
    X = embedding[mask]
    L = labels[mask]
    
    if len(set(L)) < 2:
        return {"Sil": -1.0, "DB": 10.0, "CH": 0.0, "Inertia": inertia, "Noise": list(labels).count(-1)}

    return {
        "Sil": silhouette_score(X, L),
        "DB": davies_bouldin_score(X, L),
        "CH": calinski_harabasz_score(X, L),
        "Inertia": inertia,
        "Noise": list(labels).count(-1)
    }

# --- 4. CLUSTERING ENGINE ---
def run_clustering(X_scaled, n_samples):
    best_score = -float('inf')
    best_res = None
    for nn in [5, 10, 15]:
        if nn >= n_samples: continue
        for md in [0.0, 0.1]:
            reducer = umap.UMAP(n_components=2, n_neighbors=nn, min_dist=md, random_state=42)
            embedding = reducer.fit_transform(X_scaled)
            for mcs in [2, 3, 5]:
                if mcs > n_samples: continue
                clusterer = hdbscan.HDBSCAN(min_cluster_size=mcs, min_samples=1)
                labels = clusterer.fit_predict(embedding)
                metrics = evaluate_clustering(embedding, labels)
                score = metrics['Sil'] - (0.2 * metrics['DB']) + (0.001 * metrics['CH']) - (0.5 * (metrics['Noise']/n_samples))
                if score > best_score:
                    best_score = score
                    best_res = (embedding, labels, metrics)
    return best_res

# --- 5. MAIN EXECUTION ---
def main():
    workload_dir = "/home/ubuntu/workload_clustering/workloads"
    output_dir = "/home/ubuntu/new_workload_analysis/output_final"
    if os.path.exists(output_dir): shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    all_files = glob.glob(os.path.join(workload_dir, "*.wg"))
    benchmarks = {}
    for f in all_files:
        b_name = os.path.basename(f).split('_')[0]
        if b_name not in benchmarks: benchmarks[b_name] = []
        benchmarks[b_name].append(f)
    
    all_tokens = []
    for f in all_files:
        with open(f, 'r') as file:
            for q in file:
                tokens = tokenize_query(q.strip())
                if tokens: all_tokens.append(tokens)
    ft_model = FastText(sentences=all_tokens, vector_size=100, window=5, min_count=1, workers=4)
    
    summary_results = []
    
    for b_name, files in benchmarks.items():
        if len(files) < 5: continue
        print(f"Processing {b_name}...")
        
        # --- VECTOR VERSION ---
        vec_data = [get_vector_workload_features(f) for f in files]
        vec_data = [d for d in vec_data if d]
        X_vec = StandardScaler().fit_transform(pd.DataFrame(vec_data).fillna(0))
        emb_v_umap, lbl_v, met_v = run_clustering(X_vec, len(X_vec))
        pca_v = PCA(n_components=2, random_state=42)
        emb_v_pca = pca_v.fit_transform(X_vec)
        
        # --- WORD VERSION ---
        wrd_data = [get_word_workload_embedding(f, ft_model) for f in files]
        wrd_data = [d for d in wrd_data if d is not None]
        X_wrd = StandardScaler().fit_transform(np.array(wrd_data))
        emb_w_umap, lbl_w, met_w = run_clustering(X_wrd, len(X_wrd))
        pca_w = PCA(n_components=2, random_state=42)
        emb_w_pca = pca_w.fit_transform(X_wrd)
        
        # Visualization (UMAP and PCA)
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Vector UMAP
        axes[0, 0].scatter(emb_v_umap[:, 0], emb_v_umap[:, 1], c=lbl_v, cmap='Spectral', s=40, edgecolors='white', linewidth=0.5)
        axes[0, 0].set_title(f"{b_name} - Vector (UMAP)")
        axes[0, 0].set_xlabel("UMAP Dimension 1")
        axes[0, 0].set_ylabel("UMAP Dimension 2")
        
        # Word UMAP
        axes[0, 1].scatter(emb_w_umap[:, 0], emb_w_umap[:, 1], c=lbl_w, cmap='Spectral', s=40, edgecolors='white', linewidth=0.5)
        axes[0, 1].set_title(f"{b_name} - Word (UMAP)")
        axes[0, 1].set_xlabel("UMAP Dimension 1")
        axes[0, 1].set_ylabel("UMAP Dimension 2")
        
        # Vector PCA
        axes[1, 0].scatter(emb_v_pca[:, 0], emb_v_pca[:, 1], c=lbl_v, cmap='Spectral', s=40, edgecolors='white', linewidth=0.5)
        axes[1, 0].set_title(f"{b_name} - Vector (PCA)")
        axes[1, 0].set_xlabel("Principal Component 1")
        axes[1, 0].set_ylabel("Principal Component 2")
        
        # Word PCA
        axes[1, 1].scatter(emb_w_pca[:, 0], emb_w_pca[:, 1], c=lbl_w, cmap='Spectral', s=40, edgecolors='white', linewidth=0.5)
        axes[1, 1].set_title(f"{b_name} - Word (PCA)")
        axes[1, 1].set_xlabel("Principal Component 1")
        axes[1, 1].set_ylabel("Principal Component 2")
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{b_name}_clustering_comparison.png"))
        plt.close()
        
        summary_results.append({
            "Benchmark": b_name, "Method": "Vector",
            "Silhouette": met_v['Sil'], "Davies-Bouldin": met_v['DB'], "Calinski-Harabasz": met_v['CH'], "Inertia": met_v['Inertia'], "Noise Points": met_v['Noise']
        })
        summary_results.append({
            "Benc\hmark": b_name, "Method": "Word",
            "Silhouette": met_w['Sil'], "Davies-Bouldin": met_w['DB'], "Calinski-Harabasz": met_w['CH'], "Inertia": met_w['Inertia'], "Noise Points": met_w['Noise']
        })
        
        # Pick representatives for Vector method
        picked_dir = os.path.join(output_dir, f"{b_name}_picked_workloads")
        os.makedirs(picked_dir)
        for cid in set(lbl_v):
            idx = np.where(lbl_v == cid)[0]
            if cid == -1:
                for i in idx: shutil.copy(files[i], picked_dir)
            else:
                centroid = np.mean(emb_v_umap[idx], axis=0)
                closest = idx[np.argmin(cdist(emb_v_umap[idx], centroid.reshape(1, -1)))]
                shutil.copy(files[closest], picked_dir)

    df_metrics = pd.DataFrame(summary_results)
    df_metrics.to_csv(os.path.join(output_dir, "clustering_metrics_final.csv"), index=False)
    print("Clustering and evaluation complete.")

if __name__ == "__main__":
    main()
