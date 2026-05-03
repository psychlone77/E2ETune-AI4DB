import json

nb = json.load(open('kernels/olap-dann-global/olap-dann-global.ipynb'))
src = '\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'code')

checks = {
    'domain_emb': 'domain_emb' in src,
    'residual_proj': 'residual_proj' in src,
    '512 layer': 'nn.Linear(cond_dim, 512)' in src,
    'alpha_target=3.0 (train)': 'alpha_target=3.0' in src,
    'patience=40': 'patience = 40' in src,
    'CORAL warmup 30%': 'p < 0.3' in src,
    'CORAL ramp to 60%': 'p < 0.6' in src,
    'n_epochs=150': '"n_epochs": 150' in src,
    'lr=3e-4': 'lr": 0.0003' in src or '3e-4' in src or '3e-04' in src,
    'domain_ids=domain (train)': 'domain_ids=domain' in src,
    'domain_ids=batch (eval)': 'domain_ids=batch["domain"]' in src,
    'robust_mape': '_robust_mape' in src,
    'cap_pct=200': 'cap_pct=200' in src,
}

for k, v in checks.items():
    status = "OK" if v else "MISSING"
    print(f"  [{status}]  {k}")
