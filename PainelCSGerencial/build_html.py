import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open('C:/Users/Laura/Projetos/Ideia-vesti/PainelCSGerencial/dashboard_full_data.js', 'r', encoding='utf-8') as f:
    data_js = f.read()

with open('C:/Users/Laura/Projetos/Ideia-vesti/PainelCSGerencial/template.html', 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('/*INLINE_DATA*/', data_js)

with open('C:/Users/Laura/Projetos/Ideia-vesti/PainelCSGerencial/index.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Written {len(html)//1024}KB")
