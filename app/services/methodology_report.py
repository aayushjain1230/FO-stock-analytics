"""Private developer-facing methodology report generation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from app.services.model_registry import registry_as_dict

REPORT_PATH = Path("reports/model_methodology_v3.html")


def generate_model_report(path: Path = REPORT_PATH) -> Path:
    """Generate an ignored static methodology report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    registry = registry_as_dict()
    rows = "\n".join(
        f"<tr><td>{item['name']}</td><td>{item['version']}</td><td>{item['validation_status']}</td><td>{'Active' if item['active'] else 'Disabled'}</td><td>{'; '.join(item['limitations'])}</td></tr>"
        for item in registry.values()
    )
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>V3 Model Methodology Report</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;background:#08111f;color:#e6edf7;padding:32px}}table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #26364f;padding:10px;text-align:left}}.warn{{color:#fbbf24}}</style></head>
<body><h1>Private V3 Model Methodology Report</h1>
<p>Generated {datetime.utcnow().isoformat()} UTC. This report is developer-facing and intentionally exposes limitations and disabled models.</p>
<h2>Model Inventory</h2><table><thead><tr><th>Model</th><th>Version</th><th>Validation</th><th>Status</th><th>Limitations</th></tr></thead><tbody>{rows}</tbody></table>
<h2>Known Weaknesses</h2><p class="warn">GARCH is disabled unless the optional dependency is available and validation beats simpler baselines. Pair analysis requires economic peers and can fail under structural breaks. Simulation percentages are conditional model scenarios.</p>
<h2>Baselines</h2><p>Relative value compares against random entry and correlation-only baselines. Volatility compares EWMA against recent realized-volatility baselines. Bootstrap simulations retain unconditioned and block-bootstrap baselines.</p>
</body></html>"""
    path.write_text(html, encoding="utf-8")
    return path
