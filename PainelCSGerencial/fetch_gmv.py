"""
GMV (Gross Merchandise Value) mensal por empresa em 2026. Le pedidos do
lakehouse Fabric (dbo.MongoDB_Pedidos_Geral) agrupando por (domainId, mes).
Joina com companies_data.json para trazer marca/cs/canal/cnpj.

Output: gmv_data.json

Formato (consumido pelo template.html via merge_data -> GMV_DATA):
{
    "geradoEm": "2026-04-17T...",
    "inicio": "2026-01-01",
    "empresas": [
        {
            "dominioId": "34440",
            "marca": "ESPACO DE MODA",
            "cs": "Fulana",
            "canal": "...",
            "cnpj": "...",
            "meses": {
                "2026-01": {"valPix": ..., "valCartao": ..., "valTotal": ...,
                            "qtPix": ..., "qtCartao": ..., "qtTotal": ...},
                ...
            },
            "valPix": ..., "valCartao": ..., "valTotal": ...,
            "qtPix": ..., "qtCartao": ..., "qtTotal": ...
        }
    ],
    "mesesList": ["2026-01", "2026-02", ...],
    "csList": [...]
}
"""

import json
import sys
from pathlib import Path

# Reaproveita helpers de fetch_fabric (ele ja fez o wrap de stdout utf-8 ao importar)
from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"
GMV_JSON = ROOT / "gmv_data.json"

START_DATE = "2026-01-01"
END_DATE = "2027-01-01"

SQL_GMV = f"""
SELECT domainId,
       FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM') AS mes,
       SUM(CASE WHEN payment_method = 'PIX' THEN summary_total ELSE 0 END) AS val_pix,
       SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN summary_total ELSE 0 END) AS val_cartao,
       SUM(summary_total) AS val_total,
       SUM(CASE WHEN payment_method = 'PIX' THEN 1 ELSE 0 END) AS qt_pix,
       SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
       COUNT(*) AS qt_total
FROM dbo.MongoDB_Pedidos_Geral
WHERE domainId IS NOT NULL
  AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
  AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
  AND settings_createdAt_TIMESTAMP >= '{START_DATE}'
  AND settings_createdAt_TIMESTAMP < '{END_DATE}'
GROUP BY domainId, FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM')
"""


def load_companies() -> dict[str, dict]:
    if not COMPANIES_JSON.exists():
        print(f"ERRO: {COMPANIES_JSON} nao existe. Rode fetch_fabric.py antes.", file=sys.stderr)
        sys.exit(1)
    data = json.loads(COMPANIES_JSON.read_text(encoding="utf-8"))
    # Indexa por domain_id (string) — uma matriz por dominio (isMatriz=True)
    by_dom: dict[str, dict] = {}
    for c in data:
        did = str(c.get("domain_id") or "")
        if not did:
            continue
        if c.get("isMatriz"):
            by_dom[did] = c
        elif did not in by_dom:
            # Se ainda nao registramos matriz, usa a filial temporariamente
            by_dom[did] = c
    return by_dom


def fetch_gmv(conn) -> list[dict]:
    print("[fabric] rodando query (GMV mensal por dominio)")
    cur = conn.cursor()
    cur.execute(SQL_GMV)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} linhas retornadas")
    return rows


def build(rows: list[dict], companies: dict[str, dict]) -> dict:
    from datetime import datetime, timezone

    empresas_by_dom: dict[str, dict] = {}
    meses_set: set[str] = set()
    sem_match = 0

    for r in rows:
        dom = str(r.get("domainId") or "").strip()
        if not dom:
            continue
        try:
            dom = str(int(dom))
        except (TypeError, ValueError):
            pass
        mes = r.get("mes") or ""
        if not mes:
            continue
        meses_set.add(mes)
        val_pix = float(r.get("val_pix") or 0)
        val_car = float(r.get("val_cartao") or 0)
        val_tot = float(r.get("val_total") or 0)
        qt_pix = int(r.get("qt_pix") or 0)
        qt_car = int(r.get("qt_cartao") or 0)
        qt_tot = int(r.get("qt_total") or 0)

        emp = empresas_by_dom.get(dom)
        if emp is None:
            c = companies.get(dom)
            if c is None:
                sem_match += 1
                continue
            emp = {
                "dominioId": dom,
                "marca": c.get("nome_fantasia") or c.get("name") or "",
                "cs": c.get("anjo") or "",
                "canal": c.get("canal") or "",
                "cnpj": c.get("cnpj") or "",
                "meses": {},
                "valPix": 0.0, "valCartao": 0.0, "valTotal": 0.0,
                "qtPix": 0, "qtCartao": 0, "qtTotal": 0,
            }
            empresas_by_dom[dom] = emp

        emp["meses"][mes] = {
            "valPix": round(val_pix, 2),
            "valCartao": round(val_car, 2),
            "valTotal": round(val_tot, 2),
            "qtPix": qt_pix,
            "qtCartao": qt_car,
            "qtTotal": qt_tot,
        }
        emp["valPix"] += val_pix
        emp["valCartao"] += val_car
        emp["valTotal"] += val_tot
        emp["qtPix"] += qt_pix
        emp["qtCartao"] += qt_car
        emp["qtTotal"] += qt_tot

    empresas = list(empresas_by_dom.values())
    for e in empresas:
        e["valPix"] = round(e["valPix"], 2)
        e["valCartao"] = round(e["valCartao"], 2)
        e["valTotal"] = round(e["valTotal"], 2)
    empresas.sort(key=lambda x: x["valTotal"], reverse=True)

    cs_set = {e["cs"] for e in empresas if e["cs"]}
    cs_list = sorted(cs_set, key=lambda s: s.lower())
    meses_list = sorted(meses_set)

    print(f"[build] {len(empresas)} empresas com pedidos em {START_DATE[:4]} "
          f"(sem match em companies_data.json: {sem_match} dominios)")
    total = sum(e["valTotal"] for e in empresas)
    print(f"[build] GMV total: R$ {total:,.2f}")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "inicio": START_DATE,
        "empresas": empresas,
        "mesesList": meses_list,
        "csList": cs_list,
        "resumo": {
            "nEmpresas": len(empresas),
            "totalValor": round(total, 2),
            "totalPix": round(sum(e["valPix"] for e in empresas), 2),
            "totalCartao": round(sum(e["valCartao"] for e in empresas), 2),
            "totalPedidos": sum(e["qtTotal"] for e in empresas),
        },
    }


def main() -> None:
    cfg = load_config()
    companies = load_companies()
    print(f"[companies] {len(companies)} dominios carregados de companies_data.json")
    with connect(cfg) as conn:
        rows = fetch_gmv(conn)
    data = build(rows, companies)
    GMV_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {GMV_JSON.name} ({len(data['empresas'])} empresas, "
          f"{len(data['mesesList'])} meses)")


if __name__ == "__main__":
    main()
