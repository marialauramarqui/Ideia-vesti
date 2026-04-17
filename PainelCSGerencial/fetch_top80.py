"""
Clientes Vesti (todos) que atingiram a marca de 80+ pedidos num mesmo mes
em 2026. Uma linha por (domainId, mes) que bateu o gatilho — uma empresa
pode aparecer varias vezes se bateu 80+ em meses diferentes.

Output: top80_data.json

Formato (consumido pelo template.html via merge_data -> TOP80_DATA):
{
    "geradoEm": "2026-04-17T...",
    "threshold": 80,
    "linhas": [
        {
            "dominioId": "...",
            "marca": "...",
            "cs": "...",
            "canal": "...",
            "cnpj": "...",
            "mes": "2026-02",
            "qtTotal": 120,
            "qtPix": 30,
            "qtCartao": 15,
            "valTotal": 45123.45,
            "valPix": 15000,
            "valCartao": 7500
        }
    ],
    "mesesList": ["2026-01", ...],
    "csList": [...],
    "resumo": {"nEmpresas": 34, "nMeses": 48, "totalValor": ..., ...}
}
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Reaproveita helpers de fetch_fabric (ele ja fez o wrap de stdout utf-8 ao importar)
from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"
OUT_JSON = ROOT / "top80_data.json"

THRESHOLD = 80
START_DATE = "2026-01-01"
END_DATE = "2027-01-01"

SQL_TOP80 = f"""
WITH orders AS (
    SELECT domainId, settings_createdAt_TIMESTAMP, summary_total, payment_method
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE domainId IS NOT NULL
      AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
      AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
      AND settings_createdAt_TIMESTAMP >= '{START_DATE}'
      AND settings_createdAt_TIMESTAMP < '{END_DATE}'
),
monthly AS (
    SELECT domainId,
           FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM') AS mes,
           COUNT(*) AS qt_total,
           SUM(CASE WHEN payment_method = 'PIX' THEN 1 ELSE 0 END) AS qt_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
           SUM(summary_total) AS val_total,
           SUM(CASE WHEN payment_method = 'PIX' THEN summary_total ELSE 0 END) AS val_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN summary_total ELSE 0 END) AS val_cartao
    FROM orders
    GROUP BY domainId, FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM')
    HAVING COUNT(*) >= {THRESHOLD}
)
SELECT domainId, mes, qt_total, qt_pix, qt_cartao, val_total, val_pix, val_cartao
FROM monthly
ORDER BY mes DESC, qt_total DESC
"""


def load_companies() -> dict[str, dict]:
    if not COMPANIES_JSON.exists():
        print(f"ERRO: {COMPANIES_JSON} nao existe. Rode fetch_fabric.py antes.", file=sys.stderr)
        sys.exit(1)
    data = json.loads(COMPANIES_JSON.read_text(encoding="utf-8"))
    by_dom: dict[str, dict] = {}
    for c in data:
        did = str(c.get("domain_id") or "")
        if not did:
            continue
        if c.get("isMatriz"):
            by_dom[did] = c
        elif did not in by_dom:
            by_dom[did] = c
    return by_dom


def fetch_rows(conn) -> list[dict]:
    print(f"[fabric] rodando query (empresas com {THRESHOLD}+ pedidos em algum mes 2026)")
    cur = conn.cursor()
    cur.execute(SQL_TOP80)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} (dominio, mes) bateram o gatilho")
    return rows


def build(rows: list[dict], companies: dict[str, dict]) -> dict:
    linhas: list[dict] = []
    sem_match = 0
    dominios_unicos: set[str] = set()
    meses_set: set[str] = set()
    cs_set: set[str] = set()

    for r in rows:
        dom = str(r.get("domainId") or "").strip()
        if not dom:
            continue
        try:
            dom = str(int(dom))
        except (TypeError, ValueError):
            pass
        c = companies.get(dom)
        if c is None:
            sem_match += 1
            continue
        mes = r.get("mes") or ""
        if not mes:
            continue
        cs = c.get("anjo") or ""
        linhas.append({
            "dominioId": dom,
            "marca": c.get("nome_fantasia") or c.get("name") or "",
            "cs": cs,
            "canal": c.get("canal") or "",
            "cnpj": c.get("cnpj") or "",
            "mes": mes,
            "qtTotal": int(r.get("qt_total") or 0),
            "qtPix": int(r.get("qt_pix") or 0),
            "qtCartao": int(r.get("qt_cartao") or 0),
            "valTotal": round(float(r.get("val_total") or 0), 2),
            "valPix": round(float(r.get("val_pix") or 0), 2),
            "valCartao": round(float(r.get("val_cartao") or 0), 2),
        })
        dominios_unicos.add(dom)
        meses_set.add(mes)
        if cs:
            cs_set.add(cs)

    linhas.sort(key=lambda r: (r["mes"], -r["qtTotal"]), reverse=True)

    meses_list = sorted(meses_set)
    cs_list = sorted(cs_set, key=lambda s: s.lower())

    print(f"[build] {len(linhas)} (empresa, mes) qualificados, "
          f"{len(dominios_unicos)} empresas unicas. Sem match: {sem_match}")
    total_valor = sum(l["valTotal"] for l in linhas)
    print(f"[build] GMV nos meses qualificados: R$ {total_valor:,.2f}")
    print(f"[build] Meses com algum cliente 80+: {meses_list}")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "threshold": THRESHOLD,
        "linhas": linhas,
        "mesesList": meses_list,
        "csList": cs_list,
        "resumo": {
            "nEmpresas": len(dominios_unicos),
            "nMeses": len(linhas),
            "totalValor": round(total_valor, 2),
            "totalPix": round(sum(l["valPix"] for l in linhas), 2),
            "totalCartao": round(sum(l["valCartao"] for l in linhas), 2),
            "totalPedidos": sum(l["qtTotal"] for l in linhas),
        },
    }


def main() -> None:
    cfg = load_config()
    companies = load_companies()
    print(f"[companies] {len(companies)} dominios carregados")
    with connect(cfg) as conn:
        rows = fetch_rows(conn)
    data = build(rows, companies)
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_JSON.name}")


if __name__ == "__main__":
    main()
