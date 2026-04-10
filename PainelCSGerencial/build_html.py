import sys, io
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

ROOT = Path(__file__).parent

with open(ROOT / 'dashboard_full_data.js', 'r', encoding='utf-8') as f:
    data_js = f.read()

with open(ROOT / 'template.html', 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('/*INLINE_DATA*/', data_js)

with open(ROOT / 'index.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Written {len(html)//1024}KB")
