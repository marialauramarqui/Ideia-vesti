"""
T3+ = receita de mensalidade (iugu_invoices pagas) dos clientes cujo Partner
NAO seja Starter (nem Trial/Treino), EXCLUINDO planos Vesti Start e Vesti Light
(e Starter). Categorias (espelha o Painel CS / aba Faturamento):

  plano        - Mensalidade do plano principal
  integracao   - Integração (ERP/plataforma)
  assistente   - Assistente do Vendedor
  filial       - Filiais adicionais
  outros       - Catálogo Digital, Portal Têxtil, Conecta, setup, juros, etc
  desconto     - Descontos concedidos (subtraido)
  total        - plano + integracao + assistente + filial + outros - desconto
                 (= o que o cliente paga por mes)

Agrega por mes da invoice (created_at_iso_TIMESTAMP) e por empresa.
Saida: t3plus_data.json

Rodar:
    py fetch_t3plus.py
"""

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
OUT_JSON = ROOT / "t3plus_data.json"

STARTER_ID = "c2cda592-cd9f-4380-96df-316a51bfc6fb"
TRIAL_ID = "25fec57c-620c-4ecd-ae7d-cd4fee27b158"
TREINO_ID = "ff66c2f1-1f9f-456c-9308-028e48c89582"

SQL = f"""
WITH t3_domains AS (
    SELECT d.id, d.name, d.partner_id, d.angel_id
    FROM dbo.ODBC_Domains d
    WHERE d.modulos LIKE '%vendas%'
      AND LOWER(d.name) NOT LIKE '%teste%'
      AND (d.partner_id IS NULL OR d.partner_id NOT IN (
          '{STARTER_ID}', '{TRIAL_ID}', '{TREINO_ID}'
      ))
),
ranked_companies AS (
    SELECT
        c.domain_id,
        c.company_name,
        c.social_name,
        ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
    FROM dbo.ODBC_Companies c
    WHERE c.domain_id IN (SELECT id FROM t3_domains)
),
-- UM item distinto por invoice (tabela bronze tem duplicatas por snapshot de ingestao):
itens AS (
    SELECT
        inv.id                          AS invoice_id,
        inv.customer_id                 AS customer_id,
        inv.items_id                    AS items_id,
        MAX(inv.items_description)      AS items_description,
        MAX(inv.items_price_cents)      AS item_price_cents,
        MAX(inv.items_quantity)         AS item_qty,
        MAX(inv.created_at_iso_TIMESTAMP) AS invoice_date
    FROM dbo.iugu_invoices inv
    WHERE inv.status = 'paid'
      AND inv.created_at_iso_TIMESTAMP >= '2025-01-01'
      AND inv.items_id IS NOT NULL
    GROUP BY inv.id, inv.customer_id, inv.items_id
)
SELECT
    d.id                 AS domain_id,
    d.name               AS domain_name,
    rc.company_name      AS company_name,
    rc.social_name       AS social_name,
    p.name               AS partner_name,
    a.name               AS angel_name,
    i.invoice_id         AS invoice_id,
    i.items_id           AS items_id,
    i.items_description  AS items_description,
    i.item_price_cents   AS item_price_cents,
    i.item_qty           AS item_qty,
    i.invoice_date       AS invoice_date
FROM t3_domains d
JOIN ranked_companies rc       ON rc.domain_id = d.id AND rc.rn = 1
LEFT JOIN dbo.ODBC_Partners p  ON p.id = d.partner_id
LEFT JOIN dbo.ODBC_Angels   a  ON a.id = d.angel_id
JOIN dbo.silver_companiesativos_iugu sc ON sc.domain_id = d.id
JOIN itens i                    ON i.customer_id = sc.Customer_ID_Iugu
"""


CATEGORIES = ("plano", "integracao", "assistente", "filial", "outros", "desconto")


def _cents_to_reais(c) -> float:
    if c is None:
        return 0.0
    try:
        return float(c) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _ym(dt) -> str:
    if dt is None:
        return ""
    s = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
    return s[:7]


def _brnum(s: str) -> float:
    # "1.234,56" ou "1234,56" ou "1234.56" ou "1234" -> float
    s = (s or "").strip().replace(" ", "")
    if not s:
        return 0.0
    # se tem virgula como decimal, remove pontos (milhar) e troca virgula por ponto
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


_LABEL_RE = {
    "plano":      re.compile(r"Mensalidade\s*R\$\s*([\d.,]*)", re.IGNORECASE),
    "integracao": re.compile(r"Integra[cç][aã]o\s*R\$\s*([\d.,]*)", re.IGNORECASE),
    "assistente": re.compile(r"Assistente\s+do\s+Vendedor\s*R\$\s*([\d.,]*)", re.IGNORECASE),
    "filial":     re.compile(r"Filiais?\s*R\$\s*([\d.,]*)", re.IGNORECASE),
    "desconto":   re.compile(r"Desconto\s+concedido\s*-\s*R\$\s*([\d.,]*)", re.IGNORECASE),
}

# Exclusao por descricao (planos que o time nao considera mensalidade T3+)
_EXCLUIR_RE = re.compile(
    r"(vesti\s*light|vesti\s*start\b|^\s*starter\b|assinatura:\s*starter\b|assinatura:\s*vesti\s*light|assinatura:\s*vesti\s*start)",
    re.IGNORECASE,
)


def classificar(desc: str, total: float) -> dict | None:
    """
    Retorna dict com as categorias. None se invoice deve ser totalmente ignorada
    (planos Start/Light/Starter por descricao).
    """
    out = {k: 0.0 for k in CATEGORIES}
    d = (desc or "").strip()
    d_low = d.lower()

    # Descricao estruturada: "Mensalidade R$X; Integração R$Y; Assistente ...; Filiais ...; Desconto -R$D"
    if "mensalidade" in d_low and ("integra" in d_low or "filiais" in d_low or "assistente" in d_low):
        for k, rgx in _LABEL_RE.items():
            m = rgx.search(d)
            if m:
                out[k] = _brnum(m.group(1))
        # Sanity: se tudo zero, caiu em template mas sem valores -> fallback
        if sum(out.values()) == 0:
            out["plano"] = total
        return out

    # Exclusao de planos Start/Light
    if _EXCLUIR_RE.search(d):
        return None

    # Classificacao por palavra-chave
    if "filial" in d_low or "filiais" in d_low:
        out["filial"] = total
        return out
    if "assistente" in d_low:
        out["assistente"] = total
        return out
    if "integra" in d_low:
        out["integracao"] = total
        return out
    if any(x in d_low for x in ("catálogo", "catalogo", "portal", "conecta", "setup", "juros", "multa", "adicional", "mes ", "mês ")):
        out["outros"] = total
        return out

    # Default: assinatura de plano principal (Pro, Avançado, Profissional, Básico, Essencial...)
    out["plano"] = total
    return out


def fetch_rows(conn) -> list[dict]:
    print("[fabric] rodando query T3+ (invoices paid)")
    cur = conn.cursor()
    cur.execute(SQL)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} invoices retornadas")
    return rows


def build(rows: list[dict]) -> dict:
    # Acumuladores
    serie_acc: dict[str, dict] = defaultdict(lambda: {k: 0.0 for k in CATEGORIES} | {"_invs": set(), "_emps": set()})
    empresas: dict[str, dict] = {}
    excluidos_itens = 0

    for r in rows:
        dom = str(r.get("domain_id") or "").strip()
        if not dom:
            continue
        price = _cents_to_reais(r.get("item_price_cents"))
        qty = int(r.get("item_qty") or 1)
        val = price * max(qty, 1)
        if val == 0:
            continue
        mes = _ym(r.get("invoice_date"))
        if not mes:
            continue

        cats = classificar(r.get("items_description") or "", val)
        if cats is None:
            excluidos_itens += 1
            continue

        inv_id = r.get("invoice_id") or ""

        # Soma categorias no mes
        sacc = serie_acc[mes]
        for k in CATEGORIES:
            sacc[k] += cats.get(k, 0.0)
        sacc["_invs"].add(inv_id)
        sacc["_emps"].add(dom)

        # Empresa
        emp = empresas.setdefault(dom, {
            "domainId": dom,
            "empresa": r.get("company_name") or r.get("social_name") or r.get("domain_name") or "",
            "canal": r.get("partner_name") or "",
            "cs": r.get("angel_name") or "",
            "total": 0.0,
            "totais": {k: 0.0 for k in CATEGORIES},
            "_invs": set(),
            "porMes": defaultdict(lambda: {k: 0.0 for k in CATEGORIES}),
        })
        for k in CATEGORIES:
            v = cats.get(k, 0.0)
            emp["totais"][k] += v
            emp["porMes"][mes][k] += v
        emp["_invs"].add(inv_id)
        net = cats["plano"] + cats["integracao"] + cats["assistente"] + cats["filial"] + cats["outros"] - cats["desconto"]
        emp["total"] += net

    # Serie ordenada
    def _net(acc: dict) -> float:
        return acc["plano"] + acc["integracao"] + acc["assistente"] + acc["filial"] + acc["outros"] - acc["desconto"]

    meses = sorted(serie_acc.keys())
    serie = []
    for m in meses:
        acc = serie_acc[m]
        serie.append({
            "mes": m,
            "plano": round(acc["plano"], 2),
            "integracao": round(acc["integracao"], 2),
            "assistente": round(acc["assistente"], 2),
            "filial": round(acc["filial"], 2),
            "outros": round(acc["outros"], 2),
            "desconto": round(acc["desconto"], 2),
            "total": round(_net(acc), 2),
            "nInvoices": len(acc["_invs"]),
            "nEmpresas": len(acc["_emps"]),
        })

    empresas_list = []
    for e in empresas.values():
        empresas_list.append({
            "domainId": e["domainId"],
            "empresa": e["empresa"],
            "canal": e["canal"],
            "cs": e["cs"],
            "total": round(e["total"], 2),
            "totais": {k: round(v, 2) for k, v in e["totais"].items()},
            "nInvoices": len(e["_invs"]),
            "porMes": {m: {k: round(v, 2) for k, v in d.items()} for m, d in sorted(e["porMes"].items())},
        })
    empresas_list.sort(key=lambda x: x["total"], reverse=True)

    total_geral = round(sum(s["total"] for s in serie), 2)
    ultimo = serie[-1] if serie else None
    anterior = serie[-2] if len(serie) >= 2 else None

    n_invs = len({r.get("invoice_id") for r in rows if r.get("invoice_id")})
    print(f"[build] {len(empresas_list)} empresas | {len(rows)} itens | {n_invs} invoices unicas | itens excluidos (Start/Light): {excluidos_itens}")
    print(f"[build] total T3+ desde jan/2025: R$ {total_geral:,.2f}")
    if ultimo:
        print(f"[build] {ultimo['mes']}: R$ {ultimo['total']:,.2f} "
              f"(plano {ultimo['plano']:,.0f} / filial {ultimo['filial']:,.0f} / "
              f"assist {ultimo['assistente']:,.0f} / integ {ultimo['integracao']:,.0f} / "
              f"outros {ultimo['outros']:,.0f} - desc {ultimo['desconto']:,.0f})")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "serie": serie,
        "empresas": empresas_list,
        "categorias": list(CATEGORIES),
        "resumo": {
            "totalGeral": total_geral,
            "nEmpresas": len(empresas_list),
            "nInvoices": n_invs,
            "nItensExcluidos": excluidos_itens,
            "mesAtual": ultimo["mes"] if ultimo else "",
            "mensalidadeMesAtual": ultimo["total"] if ultimo else 0.0,
            "mensalidadeMesAnterior": anterior["total"] if anterior else 0.0,
        },
    }


def main() -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        rows = fetch_rows(conn)
    data = build(rows)
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_JSON.name}")


if __name__ == "__main__":
    main()
