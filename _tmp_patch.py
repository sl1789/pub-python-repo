"""Patch notebooks to add `multifractal_empirical` references."""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# mc_vs_actual_test.ipynb: bump 'All 11' -> 'All 12' with new method name.
# ---------------------------------------------------------------------------
p1 = REPO / "databricks/jobs/mc_vs_actual_test.ipynb"
nb = json.loads(p1.read_text(encoding="utf-8"))
for c in nb["cells"]:
    src = c.get("source", [])
    for i, ln in enumerate(src):
        if "All 11 (historical" in ln:
            src[i] = ln.replace(
                "All 11 (historical, window, window_10d, window_20d, student_t, "
                "black_scholes, multifractal, block_bootstrap, fhs, fhs_rn, analogue)",
                "All 12 (historical, window, window_10d, window_20d, student_t, "
                "black_scholes, multifractal, multifractal_empirical, block_bootstrap, "
                "fhs, fhs_rn, analogue)",
            )
p1.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Patched: {p1.relative_to(REPO)}")


# ---------------------------------------------------------------------------
# monte_carlo_simulation.ipynb: add row after the `multifractal` table row.
# ---------------------------------------------------------------------------
p2 = REPO / "databricks/jobs/monte_carlo_simulation.ipynb"
nb = json.loads(p2.read_text(encoding="utf-8"))
new_row = (
    "| `multifractal_empirical` | MMAR cascade with empirical residuals: same "
    "trading-time deformation as `multifractal` but Z is resampled from historical "
    "standardised residuals (fat tails + skew from data); discounted to PV |\n"
)
patched = False
for c in nb["cells"]:
    if c.get("cell_type") != "markdown":
        continue
    src = c.get("source", [])
    already = any("multifractal_empirical" in s for s in src)
    if already:
        continue
    for i, ln in enumerate(src):
        if "| `multifractal` |" in ln:
            src.insert(i + 1, new_row)
            patched = True
            break
    if patched:
        break
p2.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Patched: {p2.relative_to(REPO)}  (added row: {patched})")


# ---------------------------------------------------------------------------
# scalability_test.ipynb: bump '(11)' -> '(12)', insert table row.
# ---------------------------------------------------------------------------
p3 = REPO / "databricks/jobs/scalability_test.ipynb"
nb = json.loads(p3.read_text(encoding="utf-8"))
target = nb["cells"][0]
assert target["cell_type"] == "markdown"
src = target["source"]
new_lines = []
inserted = False
already = any("multifractal_empirical" in s for s in src)
for ln in src:
    new_lines.append(ln.replace("## Methods Tested (11)", "## Methods Tested (12)"))
    if not already and not inserted and "| `multifractal` |" in ln:
        new_lines.append(
            "| `multifractal_empirical` | MMAR cascade with empirical residuals: "
            "same trading-time deformation as `multifractal` but Z is resampled from "
            "historical standardised residuals; discounted to PV |\n"
        )
        inserted = True
target["source"] = new_lines
p3.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Patched: {p3.relative_to(REPO)}  (inserted row: {inserted}, already: {already})")
