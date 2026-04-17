"""
GMV do 1o mes completo de empresas que viraram clientes Vesti a partir de
dez/2025. Logica:
  - "cliente Vesti" = tem pelo menos 1 pedido com summary_total > 0 (qualquer
    metodo de pagamento) no lakehouse.
  - "primeiro mes completo" = mes civil seguinte ao mes do first_at. Se cliente
    entrou em 2026-01, primeiro mes completo = 2026-02.
  - Janela: first_at >= 2025-12-01 (pra filtrar desde jan/2026 precisamos de
    clientes que entraram em dez/2025).
  - Exclui clientes cujo mes completo seja o mes corrente (ainda incompleto).

Output: gmv_data.json com shape:
{
    "geradoEm": "2026-04-17T...",
    "startFirstAt": "2025-12-01",
    "empresas": [
        {
            "dominioId": "...",
            "marca": "...",
            "cs": "...",
            "primeiroPedido": "2026-01-15",
            "mesCompleto": "2026-02",
            "meses": {"2026-02": {"valPix":..., "valCartao":..., "valTotal":...,
                                    "qtPix":..., "qtCartao":..., "qtTotal":...}, ...},
            "valPix":..., "valCartao":..., "valTotal":..., "qtPix":..., "qtCartao":..., "qtTotal":...
        }
    ],
    "mesesList": ["2026-01", "2026-02", "2026-03"],
    "csList": [...],
    "resumo": { nEmpresas, totalValor, totalPix, totalCartao, totalPedidos }
}

Cada empresa aparece 1x; valores de topo (valPix, valTotal, etc) sao do
primeiro mes completo (nao agregado em varios meses).
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Reaproveita helpers de fetch_fabric (ele ja fez o wrap de stdout utf-8 ao importar)
from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"
GMV_JSON = ROOT / "gmv_data.json"

START_FIRST_AT = "2025-12-01"   # first_at >= esta data
START_MENSAL = "2025-12-01"     # so precisamos de meses a partir daqui
END_DATE = "2027-01-01"

SQL_GMV = f"""
WITH vesti AS (
    SELECT domainId, settings_createdAt_TIMESTAMP, summary_total, payment_method
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE domainId IS NOT NULL
      AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
      AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
      AND settings_createdAt_TIMESTAMP IS NOT NULL
),
first_vesti AS (
    SELECT domainId, MIN(settings_createdAt_TIMESTAMP) AS first_at
    FROM vesti
    GROUP BY domainId
),
vesti_mensal AS (
    SELECT domainId,
           FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM') AS mes,
           SUM(CASE WHEN payment_method = 'PIX' THEN summary_total ELSE 0 END) AS val_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN summary_total ELSE 0 END) AS val_cartao,
           SUM(summary_total) AS val_total,
           SUM(CASE WHEN payment_method = 'PIX' THEN 1 ELSE 0 END) AS qt_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
           COUNT(*) AS qt_total
    FROM vesti
    WHERE settings_createdAt_TIMESTAMP >= '{START_MENSAL}'
      AND settings_createdAt_TIMESTAMP < '{END_DATE}'
    GROUP BY domainId, FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM')
)
SELECT fv.domainId,
       fv.first_at,
       vm.mes,
       vm.val_pix, vm.val_cartao, vm.val_total,
       vm.qt_pix, vm.qt_cartao, vm.qt_total
FROM first_vesti fv
LEFT JOIN vesti_mensal vm ON vm.domainId = fv.domainId
WHERE fv.first_at >= '{START_FIRST_AT}'
ORDER BY fv.domainId, vm.mes
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
    print("[fabric] rodando query (GMV 1o mes completo — clientes desde dez/2025)")
    cur = conn.cursor()
    cur.execute(SQL_GMV)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} linhas retornadas")
    return rows


def _mes_seguinte(ym: str) -> str:
    """'2026-01' -> '2026-02'; '2025-12' -> '2026-01'."""
    y, m = int(ym[:4]), int(ym[5:7])
    m += 1
    if m > 12:
        m = 1
        y += 1
    return f"{y:04d}-{m:02d}"


def build(rows: list[dict], companies: dict[str, dict]) -> dict:
    # Mes em curso — clientes cujo mesCompleto seja esse sao excluidos.
    now_utc = datetime.now(timezone.utc)
    mes_corrente = now_utc.strftime("%Y-%m")

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
        first_at = r.get("first_at")
        first_at_str = first_at.isoformat() if hasattr(first_at, "isoformat") else str(first_at or "")
        first_at_date = first_at_str[:10]
        first_at_mes = first_at_str[:7]
        if len(first_at_mes) != 7:
            continue
        mes_completo = _mes_seguinte(first_at_mes)
        if mes_completo >= mes_corrente:
            # Mes completo ainda nao fechou (ou futuro) — ignora.
            continue

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
                "primeiroPedido": first_at_date,
                "mesCompleto": mes_completo,
                "meses": {},
                "valPix": 0.0, "valCartao": 0.0, "valTotal": 0.0,
                "qtPix": 0, "qtCartao": 0, "qtTotal": 0,
            }
            empresas_by_dom[dom] = emp

        mes = r.get("mes") or ""
        if not mes:
            continue
        val_pix = float(r.get("val_pix") or 0)
        val_car = float(r.get("val_cartao") or 0)
        val_tot = float(r.get("val_total") or 0)
        qt_pix = int(r.get("qt_pix") or 0)
        qt_car = int(r.get("qt_cartao") or 0)
        qt_tot = int(r.get("qt_total") or 0)
        emp["meses"][mes] = {
            "valPix": round(val_pix, 2),
            "valCartao": round(val_car, 2),
            "valTotal": round(val_tot, 2),
            "qtPix": qt_pix,
            "qtCartao": qt_car,
            "qtTotal": qt_tot,
        }

    # Preenche os valores "topo" do empresa com os dados do mesCompleto.
    empresas: list[dict] = []
    for emp in empresas_by_dom.values():
        mv = emp["meses"].get(emp["mesCompleto"])
        if not mv:
            # Cliente nao teve pedido no mes completo — ignora (raro, mas limpo).
            continue
        emp["valPix"] = mv["valPix"]
        emp["valCartao"] = mv["valCartao"]
        emp["valTotal"] = mv["valTotal"]
        emp["qtPix"] = mv["qtPix"]
        emp["qtCartao"] = mv["qtCartao"]
        emp["qtTotal"] = mv["qtTotal"]
        meses_set.add(emp["mesCompleto"])
        empresas.append(emp)

    empresas.sort(key=lambda x: x["valTotal"], reverse=True)
    cs_set = {e["cs"] for e in empresas if e["cs"]}
    cs_list = sorted(cs_set, key=lambda s: s.lower())
    meses_list = sorted(meses_set)

    print(f"[build] {len(empresas)} empresas (first_at >= {START_FIRST_AT}, "
          f"mes completo fechado). Sem match em companies_data.json: {sem_match}")
    total = sum(e["valTotal"] for e in empresas)
    print(f"[build] GMV 1o mes completo: R$ {total:,.2f}")
    print(f"[build] Meses disponiveis: {meses_list}")

    return {
        "geradoEm": now_utc.isoformat(),
        "startFirstAt": START_FIRST_AT,
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
        rows = fetch_rows(conn)
    data = build(rows, companies)
    GMV_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {GMV_JSON.name} ({len(data['empresas'])} empresas, "
          f"{len(data['mesesList'])} meses completos)")


if __name__ == "__main__":
    main()
