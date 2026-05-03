import json
import os

NOTEBOOK_PATH = 'kernels/olap-dann-global/olap-dann-global.ipynb'
MODULES = ['data.py', 'models.py', 'eval.py', 'training.py']

try:
    with open(NOTEBOOK_PATH, 'r', encoding='utf-8') as f:
        nb = json.load(f)
except Exception:
    nb = {'cells': [], 'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}

kaggle_meta = nb.get('metadata', {}).get('kaggle', {})
kaggle_meta['isGpuEnabled'] = True
kaggle_meta['isInternetEnabled'] = True
kaggle_meta['accelerator'] = 'nvidia_tesla_t4'
nb.setdefault('metadata', {})['kaggle'] = kaggle_meta

cells = []

cells.append({
    'cell_type': 'code',
    'execution_count': None,
    'metadata': {},
    'outputs': [],
    'source': [
        '!pip install -q torch>=2.0 scikit-learn pandas numpy scipy lightgbm\n',
    ]
})

for mod in MODULES:
    with open(f'src/{mod}', 'r', encoding='utf-8') as f:
        source_code = f.read()
    
    # Deactivate relative imports since everything is now in one global notebook namespace
    source_code = source_code.replace('try:', 'if False:')
    source_code = source_code.replace('except ImportError:  # Support package-style imports (src.*)', 'elif False:')
    source_code = source_code.replace('except ImportError:', 'elif False:')

    source_lines = [line + '\n' for line in source_code.split('\n')]
    
    cells.append({
        'cell_type': 'code',
        'execution_count': None,
        'metadata': {},
        'outputs': [],
        'source': [f'# --- Module: {mod} ---\n'] + source_lines
    })

entrypoint = """import json, os, hashlib, random, time
import numpy as np
import torch
import pandas as pd

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

SEED = 42
set_seed(SEED)

try:
    df = pd.read_csv('/kaggle/input/olap-dataset-dnn-03/final_combined_olap.csv')
except FileNotFoundError:
    df = pd.read_csv('/kaggle/input/datasets/phmnmendis/olap-dataset-dnn-03/final_combined_olap.csv')

print(f'Data shape: {df.shape}')

# ===================== DANN frac=0.3 PRODUCTION RUN =====================
# Train with DANN + 30% target labels, Spearman-based early stopping
# Save best model for downstream labeling
N_FOLDS = 5
N_EPOCHS = 100

os.makedirs('/kaggle/working/artifacts', exist_ok=True)
start = time.time()

print("\\n" + "#"*70)
print("TRAINING: DANN with 30% target labels (Spearman-optimized)")
print("#"*70)

metrics, history_df, model, per_domain = train_and_eval(
    df,
    normalization_mode="per_engine",
    n_epochs=N_EPOCHS,
    batch_size=256,
    lr=5e-4,
    seed=SEED,
    device_str='cuda' if torch.cuda.is_available() else 'cpu',
    log_target=True,
    use_sampler=True,
    n_folds=N_FOLDS,
    labeled_target_frac=0.3,
    use_dann=True,
)

# Save best model
if model is not None:
    torch.save(
        {
            'state_dict': model.state_dict(),
            'config': {
                'labeled_target_frac': 0.3,
                'use_dann': True,
                'n_epochs': N_EPOCHS,
                'n_folds': N_FOLDS,
                'seed': SEED,
                'normalization_mode': 'per_engine',
                'early_stopping': 'spearman',
            },
            'metrics': {
                'pooled': metrics['pooled'],
                'per_domain': metrics['per_domain'],
                'best_epoch': metrics['best_epoch'],
                'train_minutes': metrics['train_minutes'],
            }
        },
        '/kaggle/working/artifacts/dann_frac03_best.pt'
    )
    print("\\nModel saved to /kaggle/working/artifacts/dann_frac03_best.pt")

# Save history
history_df.to_csv('/kaggle/working/artifacts/history_dann_frac03.csv', index=False)

# Save full metrics
with open('/kaggle/working/artifacts/metrics_dann_frac03.json', 'w') as f:
    json.dump({
        'pooled': metrics['pooled'],
        'per_domain': metrics['per_domain'],
        'best_epoch': metrics['best_epoch'],
        'train_minutes': metrics['train_minutes'],
        'fold_metrics': [{
            'fold': fm['fold'],
            'pooled_mape': fm['pooled_mape'],
            'pooled_rmse': fm['pooled_rmse'],
            'pooled_spearman': fm['pooled_spearman'],
            'best_epoch': fm['best_epoch'],
            'train_minutes': fm['train_minutes'],
            'per_domain': fm['per_domain'],
        } for fm in metrics.get('fold_metrics', [])],
    }, f, indent=2, default=str)

total_min = (time.time() - start) / 60.0
p = metrics['pooled']
print(f"\\n{'='*70}")
print(f"FINAL RESULTS (DANN frac=0.3, {N_FOLDS}-fold, {total_min:.1f} min)")
print(f"{'='*70}")
print(f"  Pooled MAPE:     {p['mape']:.2f}%")
print(f"  Pooled Spearman: {p['spearman']:.4f}")
print(f"  Best Epoch:      {metrics['best_epoch']}")

print(f"\\nPer-domain:")
for d in ['0', '1', '2', '3']:
    if d in metrics['per_domain']:
        dm = metrics['per_domain'][d]
        print(f"  D{d}: MAPE={dm.get('mape_mean', float('nan')):.2f}% | Spearman={dm.get('spearman_mean', float('nan')):.4f}")

print(f"\\nDONE - total time: {total_min:.1f} minutes ({total_min/60:.1f} hours)")
"""

cells.append({
    'cell_type': 'code',
    'execution_count': None,
    'metadata': {},
    'outputs': [],
    'source': [line + '\n' for line in entrypoint.split('\n')]
})

nb['cells'] = cells

with open(NOTEBOOK_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print('Flat Notebook compiled without git clone or writefiles!')
