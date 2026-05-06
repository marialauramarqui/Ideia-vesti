"""
Enriquece paVesti dos onlog_diff_*.json com `marcasInferidas`: lista de
marcas onde o cliente (CPF/CNPJ) ja fez pedido no Vesti, ordenada por
quantidade de pedidos. Permite descobrir de qual marca veio uma postagem
avulsa PA VESTI mesmo sem orderNumber/domainId.

Uso: py enrich_pavesti_marcas.py
"""

import json
import re
import sys
from pathlib import Path

from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"


def norm_doc(s) -> str:
    return re.sub(r"\D", "", str(s or ""))


def load_marca_by_dom() -> dict[str, str]:
    data = json.loads(COMPANIES_JSON.read_text(encoding="utf-8"))
    by: dict[str, str] = {}
    for c in data:
        did = str(c.get("domain_id") or "").strip()
        if not did:
            continue
        nome = c.get("nome_fantasia") or c.get("name") or ""
        if c.get("isMatriz") or did not in by:
            by[did] = nome
    return by


def fetch_marcas_por_doc(conn, docs: list[str]) -> dict[str, list[tuple[str, int]]]:
    """Retorna {doc_normalizado: [(domainId, n), ...] ordenado desc}."""
    if not docs:
        return {}
    out: dict[str, list[tuple[str, int]]] = {}
    BATCH = 500
    for i in range(0, len(docs), BATCH):
        chunk = docs[i:i + BATCH]
        placeholders = ",".join("?" for _ in chunk)
        sql = f"""
            SELECT doc_norm, domainId, COUNT(*) AS n
            FROM (
                SELECT
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(customer_doc,'.',''),'-',''),'/',''),' ',''),CHAR(9),'') AS doc_norm,
                    domainId
                FROM dbo.MongoDB_Pedidos_Geral
                WHERE customer_doc IS NOT NULL AND customer_doc <> ''
                  AND domainId IS NOT NULL
            ) t
            WHERE doc_norm IN ({placeholders})
            GROUP BY doc_norm, domainId
        """
        cur = conn.cursor()
        cur.execute(sql, chunk)
        for doc, dom, n in cur.fetchall():
            k = norm_doc(doc)
            try:
                dom_str = str(int(dom))
            except (TypeError, ValueError):
                dom_str = str(dom or "")
            out.setdefault(k, []).append((dom_str, int(n)))
    for k in out:
        out[k].sort(key=lambda x: -x[1])
    return out


def enrich_diff(path: Path, marcas_dom: dict[str, list[tuple[str, int]]], marca_by_dom: dict[str, str]) -> int:
    d = json.loads(path.read_text(encoding="utf-8"))
    n = 0
    for x in d.get("paVesti", []) or []:
        doc = norm_doc(x.get("clienteDoc"))
        if not doc:
            continue
        hits = marcas_dom.get(doc) or []
        if not hits:
            x["marcasInferidas"] = []
            continue
        x["marcasInferidas"] = [
            {"dominioId": dom, "marca": marca_by_dom.get(dom, ""), "n": cnt}
            for dom, cnt in hits[:5]
        ]
        n += 1
    snap = (d.get("_planilhaSnapshot") or {}).get("paVesti") or []
    for x in snap:
        doc = norm_doc(x.get("clienteDoc"))
        if not doc:
            continue
        hits = marcas_dom.get(doc) or []
        x["marcasInferidas"] = [
            {"dominioId": dom, "marca": marca_by_dom.get(dom, ""), "n": cnt}
            for dom, cnt in hits[:5]
        ]
    path.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return n


def main() -> None:
    diffs = sorted(ROOT.glob("onlog_diff_*.json"))
    if not diffs:
        print("nenhum onlog_diff_*.json encontrado", file=sys.stderr)
        return
    docs: set[str] = set()
    for p in diffs:
        d = json.loads(p.read_text(encoding="utf-8"))
        for x in d.get("paVesti", []) or []:
            doc = norm_doc(x.get("clienteDoc"))
            if doc:
                docs.add(doc)
    print(f"[scan] {len(docs)} CPFs/CNPJs unicos em PA VESTI")
    if not docs:
        return
    marca_by_dom = load_marca_by_dom()
    cfg = load_config()
    with connect(cfg) as conn:
        marcas_dom = fetch_marcas_por_doc(conn, sorted(docs))
    com_match = sum(1 for k in docs if marcas_dom.get(k))
    print(f"[fabric] match em pedidos: {com_match}/{len(docs)}")
    for p in diffs:
        n = enrich_diff(p, marcas_dom, marca_by_dom)
        print(f"[write] {p.name}: {n} PA VESTI enriquecidos")


if __name__ == "__main__":
    main()
