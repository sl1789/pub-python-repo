import json
nb = json.load(open(r"databricks/jobs/monte_carlo_simulation.ipynb", encoding="utf-8"))
for i, c in enumerate(nb["cells"]):
    src = "".join(c["source"])
    if "multifractal" in src or "block_bootstrap" in src or "analogue" in src:
        print(f"--- cell {i} ({c['cell_type']}) ---")
        print(src)
        print("--- end ---")
