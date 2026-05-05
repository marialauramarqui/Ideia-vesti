"""
Detecta churn REAL por produto, por mes e por CS, a partir de janeiro/2026.

Regra:
  - Pra cada empresa x produto (iugu subscription), pega a ultima fatura paga
  - Se a ultima fatura paga foi ha 2 meses ou mais (configuravel), a empresa
    eh considerada churn daquele produto
  - churn_month = mes da ultima fatura + 1
  - Filtra churn_month entre 2026-01 e o mes atual

Saidas:
  - churn_real.csv         -> uma linha por empresa x produto em churn
  - churn_real_pivot.csv   -> agregado por AnoMes x Produto x CS

Rodar:
    py fetch_churn.py
    py fetch_churn.py --meses 3      # usa corte de 3 meses em vez de 2
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from collections import Counter, defaultdict
from pathlib import Path

from fetch_fabric import load_config, connect  # reusa auth + conexao

ROOT = Path(__file__).parent
OUT_DETAIL = ROOT / "churn_real.csv"
OUT_PIVOT = ROOT / "churn_real_pivot.csv"
OUT_TOTAL = ROOT / "churn_total.csv"
OUT_GERAL = ROOT / "churn_geral.json"

START_MONTH = (2026, 1)  # filtro de churn_month minimo (Jan/2026)

SQL = """
WITH active_domains AS (
    SELECT d.id, d.name, d.angel_id
    FROM dbo.ODBC_Domains d
    WHERE d.modulos LIKE '%vendas%'
      AND (d.partner_id IS NULL OR d.partner_id NOT IN (
          'ff66c2f1-1f9f-456c-9308-028e48c89582',
          '25fec57c-620c-4ecd-ae7d-cd4fee27b158'
      ))
      AND LOWER(d.name) NOT LIKE '%teste%'
),
subs AS (
    SELECT
        sc.domain_id,
        s.id           AS subscription_id,
        s.plan_name,
        s.customer_id,
        LOWER(s.active)    AS active_flag,
        LOWER(s.suspended) AS suspended_flag
    FROM dbo.silver_companiesativos_iugu sc
    JOIN dbo.iugu_subscriptions s ON s.customer_id = sc.Customer_ID_Iugu
),
last_inv_sub AS (
    SELECT
        inv.subscription_id,
        MAX(inv.created_at_iso_TIMESTAMP) AS last_paid_at
    FROM dbo.iugu_invoices inv
    WHERE inv.status = 'paid' AND inv.subscription_id IS NOT NULL
    GROUP BY inv.subscription_id
)
SELECT
    d.id                  AS domain_id,
    d.name                AS domain_name,
    a.name                AS cs_name,
    subs.subscription_id,
    subs.plan_name,
    subs.active_flag,
    subs.suspended_flag,
    last_inv_sub.last_paid_at
FROM active_domains d
JOIN subs ON subs.domain_id = d.id
LEFT JOIN last_inv_sub ON last_inv_sub.subscription_id = subs.subscription_id
LEFT JOIN dbo.ODBC_Angels a ON a.id = d.angel_id
"""


def _to_date(v) -> dt.date | None:
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip().replace("Z", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s[: len(fmt) + 6 if "%f" in fmt else len(fmt)], fmt).date()
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(s).date()
    except Exception:
        return None


def _months_ago(d: dt.date, n: int) -> dt.date:
    m = d.month - n
    y = d.year
    while m <= 0:
        m += 12
        y -= 1
    return dt.date(y, m, 1)


def _add_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _norm_plan(s: str) -> str:
    import unicodedata
    nfd = unicodedata.normalize("NFD", str(s or ""))
    no_diac = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return no_diac.lower().replace("_", " ").strip()


# Classificacao dos planos:
#   ('addon', 0)     -> nao e' plano principal (Oraculo, Ass. do Vendedor, Filial Atacado)
#   ('base', 1)      -> base baixo (Starter, Vesti Light, Plano Essencial)
#   ('base', 2)      -> base alto (Plano PRO, Vesti Pro, Plano Avancado)
# Regras (definidas com Laura):
#   - Se a empresa parou um plano base de tier N e continua em outro plano base de tier >= N
#     => UPGRADE (ou lateral): nao e' churn, sai das listas
#   - Se a empresa parou plano(s) base e so sobrou add-on => CHURN TOTAL
#     (perdeu o plano principal, addon sozinho nao representa relacao ativa)
#   - Caso contrario (perdeu addon mantendo base, ou downgrade) => CHURN PARCIAL
PLAN_KEYWORDS_ADDON = ["oraculo", "assitente do vendedor", "assistente do vendedor", "filial atacado"]
PLAN_KEYWORDS_TIER2 = ["plano pro", "plano_pro", "vesti pro", "vesti_pro", "plano avancado"]


def classify_plan(name: str) -> tuple[str, int]:
    n = _norm_plan(name)
    for k in PLAN_KEYWORDS_ADDON:
        if k in n:
            return ("addon", 0)
    for k in PLAN_KEYWORDS_TIER2:
        if k in n:
            return ("base", 2)
    # default: base baixo (Starter, Vesti Light, Plano Essencial e desconhecidos)
    return ("base", 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meses", type=int, default=3,
                    help="Corte: ultima fatura tem que ser ha pelo menos N meses pra contar como churn (default 3)")
    args = ap.parse_args()

    cfg = load_config()
    with connect(cfg) as conn:
        print("[churn] rodando query...")
        cur = conn.cursor()
        cur.execute(SQL)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        print(f"[churn] {len(rows)} linhas de subscription retornadas")

    today = dt.date.today()
    cutoff_month = _months_ago(today, args.meses)  # 1o dia do mes de corte
    start_month = dt.date(*START_MONTH, 1)
    current_month_start = today.replace(day=1)

    # Agrupa por (domain, plan) pegando a MAIOR data de fatura paga (caso haja subs duplicadas)
    latest: dict[tuple[str, str], dict] = {}
    for r in rows:
        did = str(r.get("domain_id") or "")
        plan = (r.get("plan_name") or "").strip()
        if not did or not plan:
            continue
        last = _to_date(r.get("last_paid_at"))
        key = (did, plan)
        prev = latest.get(key)
        if prev is None or (last and (prev["last_paid"] is None or last > prev["last_paid"])):
            latest[key] = {
                "domain_id": did,
                "domain_name": r.get("domain_name") or "",
                "cs_name": r.get("cs_name") or "",
                "plan_name": plan,
                "last_paid": last,
            }

    # Agrega por dominio pra detectar churn TOTAL
    # (dominio onde TODAS as subs com fatura paga estao em churn).
    by_domain: dict[str, list[dict]] = defaultdict(list)
    for item in latest.values():
        by_domain[item["domain_id"]].append(item)

    domain_status: dict[str, str] = {}  # "total" | "parcial" | "ativo"
    domain_max_last: dict[str, dt.date] = {}  # maior last_paid do dominio
    for did, items in by_domain.items():
        with_inv = [i for i in items if i["last_paid"] is not None]
        if not with_inv:
            domain_status[did] = "sem_fatura"
            continue
        maxd = max(i["last_paid"] for i in with_inv)
        domain_max_last[did] = maxd
        all_churn = all(dt.date(i["last_paid"].year, i["last_paid"].month, 1) < cutoff_month
                        for i in with_inv)
        any_churn = any(dt.date(i["last_paid"].year, i["last_paid"].month, 1) < cutoff_month
                        for i in with_inv)
        if all_churn:
            domain_status[did] = "total"
        elif any_churn:
            domain_status[did] = "parcial"
        else:
            domain_status[did] = "ativo"

    churn_rows = []
    sem_fatura = 0
    muito_recente = 0
    fora_janela = 0

    for item in latest.values():
        last = item["last_paid"]
        if last is None:
            sem_fatura += 1
            continue
        last_month = dt.date(last.year, last.month, 1)
        if last_month >= cutoff_month:
            muito_recente += 1
            continue
        cy, cm = _add_month(last.year, last.month)
        churn_month = dt.date(cy, cm, 1)
        if churn_month < start_month or churn_month > current_month_start:
            fora_janela += 1
            continue
        did = item["domain_id"]
        churn_rows.append({
            "AnoMes": f"{cy:04d}-{cm:02d}",
            "Produto": item["plan_name"],
            "CS": item["cs_name"],
            "Empresa": item["domain_name"],
            "DomainId": did,
            "UltimaFatura": last.isoformat(),
            "ChurnTotal": "Sim" if domain_status.get(did) == "total" else "Nao",
        })

    churn_rows.sort(key=lambda x: (x["AnoMes"], x["Produto"], x["CS"], x["Empresa"]))

    # churn TOTAL: uma linha por dominio, usando a ULTIMA data de fatura do dominio
    total_rows = []
    for did, status in domain_status.items():
        if status != "total":
            continue
        last = domain_max_last.get(did)
        if last is None:
            continue
        cy, cm = _add_month(last.year, last.month)
        churn_month = dt.date(cy, cm, 1)
        if churn_month < start_month or churn_month > current_month_start:
            continue
        # pega nome/cs de qualquer item do dominio
        sample = by_domain[did][0]
        produtos = sorted({i["plan_name"] for i in by_domain[did] if i["last_paid"] is not None})
        total_rows.append({
            "AnoMes": f"{cy:04d}-{cm:02d}",
            "Empresa": sample["domain_name"],
            "DomainId": did,
            "CS": sample["cs_name"],
            "ProdutosQueTinha": " | ".join(produtos),
            "UltimaFatura": last.isoformat(),
        })
    total_rows.sort(key=lambda x: (x["AnoMes"], x["CS"], x["Empresa"]))

    # === churn_geral.json: lista total + lista parcial pra aba Churn Geral ===
    # Aplica classificacao de tier dos planos (ver classify_plan):
    #  - upgrade/lateral entre bases => nao e' churn
    #  - so sobrou addon (base inteira parou) => total
    #  - resto (perdeu addon mantendo base, ou downgrade base) => parcial
    geral_total = []
    geral_parcial = []
    upgrade_count = 0
    for did, items in by_domain.items():
        with_inv = [i for i in items if i["last_paid"] is not None]
        if not with_inv:
            continue
        # parou = last_paid < cutoff_month; continua = >= cutoff
        parou, continua = [], []
        for i in with_inv:
            lp = i["last_paid"]
            lp_month = dt.date(lp.year, lp.month, 1)
            kind, tier = classify_plan(i["plan_name"])
            entry = {
                "produto": i["plan_name"],
                "ultimaFatura": lp.isoformat(),
                "_kind": kind, "_tier": tier,
            }
            (parou if lp_month < cutoff_month else continua).append(entry)
        if not parou:
            continue  # nada parou - nao e' churn
        # Tiers
        max_tier_base_parou = max((p["_tier"] for p in parou if p["_kind"] == "base"), default=0)
        max_tier_base_continua = max((p["_tier"] for p in continua if p["_kind"] == "base"), default=0)
        any_base_continua = max_tier_base_continua > 0
        # 1) Upgrade/lateral: tem base que continua e tier >= tier do que parou
        if max_tier_base_parou > 0 and any_base_continua and max_tier_base_continua >= max_tier_base_parou:
            upgrade_count += 1
            continue
        # Mes do churn (maior last_paid dos que pararam)
        max_parou_d = max(dt.date.fromisoformat(p["ultimaFatura"]) for p in parou)
        cy, cm = _add_month(max_parou_d.year, max_parou_d.month)
        churn_month_iso = f"{cy:04d}-{cm:02d}"
        churn_date = dt.date(cy, cm, 1)
        if churn_date < start_month or churn_date > current_month_start:
            continue
        # Limpa marcadores internos antes de serializar
        def _clean(lst):
            return sorted([{"produto": x["produto"], "ultimaFatura": x["ultimaFatura"]} for x in lst],
                          key=lambda x: x["produto"])
        sample = items[0]
        rec = {
            "domainId": did,
            "empresa": sample["domain_name"],
            "cs": sample["cs_name"],
            "anoMesChurn": churn_month_iso,
            "ultimaFaturaProdutosParou": max_parou_d.isoformat(),
            "produtosParou": _clean(parou),
            "produtosContinua": _clean(continua),
            "nProdutosParou": len(parou),
            "nProdutosContinua": len(continua),
        }
        # 2) Total: nao tem base ativa OU nao tem nada continuando
        if not continua or not any_base_continua:
            geral_total.append(rec)
        else:
            # 3) Parcial: tem base continuando mas nao cobre o que parou (downgrade ou perdeu addon)
            geral_parcial.append(rec)
    geral_total.sort(key=lambda x: (x["anoMesChurn"], x["cs"], x["empresa"]))
    geral_parcial.sort(key=lambda x: (x["anoMesChurn"], x["cs"], x["empresa"]))
    print(f"[churn-geral] upgrades/laterais ignorados: {upgrade_count}")

    geral_out = {
        "geradoEm": dt.datetime.now().isoformat(),
        "cortemeses": args.meses,
        "janelaInicio": start_month.isoformat(),
        "total": geral_total,
        "parcial": geral_parcial,
    }
    import json as _json
    OUT_GERAL.write_text(_json.dumps(geral_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] {OUT_GERAL.name} (total={len(geral_total)} parcial={len(geral_parcial)})")

    with OUT_TOTAL.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["AnoMes", "Empresa", "DomainId", "CS",
                                          "ProdutosQueTinha", "UltimaFatura"])
        w.writeheader()
        w.writerows(total_rows)

    with OUT_DETAIL.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["AnoMes", "Produto", "CS", "Empresa",
                                          "DomainId", "UltimaFatura", "ChurnTotal"])
        w.writeheader()
        w.writerows(churn_rows)

    pivot: Counter[tuple[str, str, str]] = Counter()
    for r in churn_rows:
        pivot[(r["AnoMes"], r["Produto"], r["CS"])] += 1

    with OUT_PIVOT.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["AnoMes", "Produto", "CS", "ClientesChurn"])
        for k, v in sorted(pivot.items()):
            w.writerow([*k, v])

    print(f"[churn] corte: ultima fatura antes de {cutoff_month.isoformat()} ({args.meses} meses atras)")
    print(f"[churn] janela churn_month: {start_month.isoformat()} a {current_month_start.isoformat()}")
    print(f"[churn] subs unicos (domain x plan): {len(latest)}")
    print(f"[churn]   sem fatura paga: {sem_fatura}")
    print(f"[churn]   muito recente (nao churn): {muito_recente}")
    print(f"[churn]   fora da janela (antes de 2026-01): {fora_janela}")
    print(f"[churn]   => churn real: {len(churn_rows)}")
    print(f"[churn]   => churn TOTAL (parou tudo): {len(total_rows)}")
    print(f"[ok] {OUT_DETAIL.name} ({len(churn_rows)} linhas)")
    print(f"[ok] {OUT_PIVOT.name} ({len(pivot)} linhas)")
    print(f"[ok] {OUT_TOTAL.name} ({len(total_rows)} linhas)")


if __name__ == "__main__":
    main()
