from datetime import date, datetime, timedelta

import requests
import streamlit as st

BASE_URL = "https://api.iugu.com/v1"

st.set_page_config(page_title="Vesti - Pix Automático", page_icon="💸", layout="wide")


def check_password():
    def password_entered():
        if st.session_state.get("password") == st.secrets.get("app_password"):
            st.session_state["auth_ok"] = True
            st.session_state["password"] = ""
        else:
            st.session_state["auth_ok"] = False

    if st.session_state.get("auth_ok"):
        return True

    st.title("🔒 Acesso restrito")
    st.text_input("Senha", type="password", key="password", on_change=password_entered)
    if st.session_state.get("auth_ok") is False:
        st.error("Senha incorreta.")
    return False


def carregar_parceiros():
    parceiros = st.secrets.get("parceiros", [])
    return [dict(p) for p in parceiros]


def selecionar_parceiro(parceiros, key):
    nomes = [p["nome"] for p in parceiros]
    parceiro_nome = st.selectbox("Parceiro (conta iugu)", nomes, key=key)
    return next(p for p in parceiros if p["nome"] == parceiro_nome)


def criar_fatura(token, dados):
    payload = {
        "email": dados["email"],
        "due_date": dados["due_date"].isoformat(),
        "items": [
            {
                "description": dados["descricao"],
                "quantity": 1,
                "price_cents": dados["valor_cents"],
            }
        ],
        "payer": {
            "name": dados["nome"],
            "cpf_cnpj": dados["cpf"],
            "email": dados["email"],
        },
        "automatic_pix": {
            "journey": dados["journey"],
            "frequency": dados["frequencia"],
            "recurrence_beginning": dados["recurrence_beginning"].isoformat(),
            "contract_number": dados["contract_number"][:35],
        },
    }
    r = requests.post(
        f"{BASE_URL}/invoices",
        auth=(token, ""),
        json=payload,
        timeout=30,
    )
    return r, payload


def listar_faturas(token, data_inicio, data_fim, limit=30):
    params = {
        "limit": limit,
        "start": 0,
        "sortBy[created_at]": "desc",
        "created_at_from": data_inicio.isoformat(),
        "created_at_to": data_fim.isoformat(),
    }
    r = requests.get(
        f"{BASE_URL}/invoices",
        auth=(token, ""),
        params=params,
        timeout=30,
    )
    return r


def buscar_detalhes_faturas(token, items):
    detalhadas = []
    for item in items:
        inv_id = item.get("id")
        if not inv_id:
            detalhadas.append(item)
            continue
        try:
            r = consultar_fatura(token, inv_id)
            if r.status_code < 400:
                detalhadas.append(r.json())
            else:
                detalhadas.append(item)
        except requests.RequestException:
            detalhadas.append(item)
    return detalhadas


def consultar_fatura(token, invoice_id):
    r = requests.get(
        f"{BASE_URL}/invoices/{invoice_id}",
        auth=(token, ""),
        timeout=30,
    )
    return r


def classificar_fatura(inv):
    status = (inv.get("status") or "").lower()
    auto = inv.get("automatic_pix") or {}
    tem_auto = bool(auto)
    pago = status == "paid"
    if pago and tem_auto:
        return "🟢 Pago + recorrência"
    if pago and not tem_auto:
        return "🟡 Pago SEM recorrência"
    if status == "pending":
        return "⚪ Aguardando pagamento"
    if status in ("canceled", "expired"):
        return f"⚫ {status.capitalize()}"
    return f"❔ {status or 'desconhecido'}"


def pagina_gerar(parceiros):
    st.subheader("Dados do cliente")
    parceiro = selecionar_parceiro(parceiros, key="parceiro_gerar")

    with st.form("fatura_form"):
        col1, col2 = st.columns(2)
        with col1:
            nome = st.text_input("Nome completo*")
            email = st.text_input("Email*")
            cpf = st.text_input("CPF/CNPJ* (só números)")
            descricao = st.text_input("Descrição*", value="Assinatura mensal")
        with col2:
            valor = st.number_input(
                "Valor (R$)*", min_value=0.01, value=49.90, step=0.10, format="%.2f"
            )
            frequencia_label = st.selectbox(
                "Frequência",
                ["Mensal", "Semanal", "Trimestral", "Semestral", "Anual"],
                index=0,
            )
            recurrence_beginning = st.date_input(
                "Início da recorrência (1ª cobrança)",
                value=date.today() + timedelta(days=1),
                min_value=date.today() + timedelta(days=1),
            )
            due_date = recurrence_beginning

        contract_number = st.text_input(
            "Número do contrato (opcional, máx. 35 chars)",
            value=f"CTR-{date.today().strftime('%Y%m%d')}",
        )

        st.info(
            "🔁 O QR Code gerado cobra a 1ª parcela **e** já autoriza a recorrência "
            "automaticamente no mesmo ato — o cliente não precisa habilitar nada."
        )

        submitted = st.form_submit_button("🚀 Gerar Pix Automático", type="primary")

    if not submitted:
        return

    cpf_limpo = "".join(filter(str.isdigit, cpf))
    if not (nome and email and cpf_limpo and descricao):
        st.error("Preencha todos os campos obrigatórios (*).")
        return

    freq_map = {
        "Semanal": "weekly",
        "Mensal": "monthly",
        "Trimestral": "quarterly",
        "Semestral": "semiannually",
        "Anual": "yearly",
    }

    dados = {
        "nome": nome.strip(),
        "email": email.strip(),
        "cpf": cpf_limpo,
        "descricao": descricao.strip(),
        "valor_cents": int(round(valor * 100)),
        "frequencia": freq_map[frequencia_label],
        "due_date": due_date,
        "recurrence_beginning": recurrence_beginning,
        "contract_number": contract_number.strip() or f"CTR-{cpf_limpo}",
        "journey": 3,
    }

    with st.spinner(f"Criando fatura em {parceiro['nome']}..."):
        try:
            r, payload_enviado = criar_fatura(parceiro["token"], dados)
        except requests.RequestException as e:
            st.error(f"Erro de conexão: {e}")
            return

    if r.status_code >= 400:
        st.error(f"Erro {r.status_code} ao criar fatura")
        try:
            st.json(r.json())
        except Exception:
            st.code(r.text)
        return

    data = r.json()
    pix = data.get("pix") or {}
    auto = data.get("automatic_pix") or {}

    st.success("✅ Fatura criada com sucesso!")

    st.markdown(f"**Parceiro:** {parceiro['nome']}")
    st.markdown(f"**Invoice ID:** `{data.get('id')}`")
    st.markdown(f"**Status:** {data.get('status')}")
    st.markdown(f"**Valor:** R$ {(data.get('total_cents') or 0)/100:.2f}")
    if auto.get("receiver_recurrence_id"):
        st.markdown(f"**Recurrence ID:** `{auto['receiver_recurrence_id']}`")

    qr_img = pix.get("qrcode")
    qr_text = pix.get("qrcode_text")

    st.divider()
    st.subheader("🔁 Pix Automático — pagamento + recorrência")
    if qr_img or qr_text:
        if qr_img:
            st.image(qr_img, caption="QR Code - Pix Automático (pagamento + recorrência)", width=260)
        if qr_text:
            st.markdown("**Código copia e cola:**")
            st.code(qr_text, language=None)
        st.info(
            "📱 Compartilhe este QR Code ou o link de pagamento com o cliente. "
            "Na jornada 3, o mesmo QR cobra a 1ª parcela e autoriza a recorrência."
        )
    else:
        st.warning("⚠️ Nenhum QR Code foi retornado pela iugu.")
        st.json({"pix": pix, "automatic_pix": auto})

    if data.get("secure_url"):
        st.link_button("🔗 Abrir página de pagamento iugu", data["secure_url"])


def pagina_conferir(parceiros):
    st.subheader("📋 Conferir faturas")
    st.caption(
        "Liste as faturas recentes e veja quais foram pagas **com** recorrência e quais "
        "foram pagas **sem** recorrência (🟡 = precisa de atenção)."
    )

    parceiro = selecionar_parceiro(parceiros, key="parceiro_conferir")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        data_inicio = st.date_input(
            "De", value=date.today() - timedelta(days=7), key="data_inicio"
        )
    with col2:
        data_fim = st.date_input("Até", value=date.today(), key="data_fim")
    with col3:
        st.write("")
        st.write("")
        buscar = st.button("🔍 Buscar faturas", type="primary")

    if not buscar:
        return

    with st.spinner(f"Consultando faturas de {parceiro['nome']}..."):
        try:
            r = listar_faturas(parceiro["token"], data_inicio, data_fim)
        except requests.RequestException as e:
            st.error(f"Erro de conexão: {e}")
            return

    if r.status_code >= 400:
        st.error(f"Erro {r.status_code} ao listar faturas")
        try:
            st.json(r.json())
        except Exception:
            st.code(r.text)
        return

    resp = r.json()
    items_basicos = resp.get("items") or []

    if not items_basicos:
        st.info("Nenhuma fatura encontrada no período.")
        return

    with st.spinner(f"Buscando detalhes de {len(items_basicos)} fatura(s)..."):
        items = buscar_detalhes_faturas(parceiro["token"], items_basicos)

    st.success(f"{len(items)} fatura(s) encontrada(s).")

    total = len(items)
    pagos_sem_rec = sum(
        1
        for i in items
        if (i.get("status") or "").lower() == "paid" and not (i.get("automatic_pix") or {})
    )
    pagos_com_rec = sum(
        1
        for i in items
        if (i.get("status") or "").lower() == "paid" and (i.get("automatic_pix") or {})
    )
    pendentes = sum(1 for i in items if (i.get("status") or "").lower() == "pending")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total", total)
    k2.metric("🟢 Pagos c/ recorrência", pagos_com_rec)
    k3.metric("🟡 Pagos SEM recorrência", pagos_sem_rec)
    k4.metric("⚪ Pendentes", pendentes)

    if pagos_sem_rec > 0:
        st.warning(
            f"⚠️ {pagos_sem_rec} fatura(s) foram pagas mas a recorrência "
            "**NÃO** foi autorizada pelo cliente. Verifique abaixo."
        )

    filtro = st.radio(
        "Filtrar",
        ["Todas", "🟡 Apenas pagas SEM recorrência", "🟢 Apenas pagas com recorrência", "⚪ Apenas pendentes"],
        horizontal=True,
    )

    def passa_filtro(inv):
        status = (inv.get("status") or "").lower()
        tem_auto = bool(inv.get("automatic_pix") or {})
        if filtro == "Todas":
            return True
        if filtro == "🟡 Apenas pagas SEM recorrência":
            return status == "paid" and not tem_auto
        if filtro == "🟢 Apenas pagas com recorrência":
            return status == "paid" and tem_auto
        if filtro == "⚪ Apenas pendentes":
            return status == "pending"
        return True

    filtradas = [i for i in items if passa_filtro(i)]

    linhas = []
    for inv in filtradas:
        auto = inv.get("automatic_pix") or {}
        linhas.append(
            {
                "Situação": classificar_fatura(inv),
                "Criada em": inv.get("created_at") or inv.get("created_at_iso"),
                "Cliente": inv.get("payer_name") or "—",
                "Email": inv.get("payer_email") or "—",
                "Valor": inv.get("total") or f"R$ {(inv.get('total_cents') or 0)/100:.2f}",
                "Pago em": inv.get("paid_at") or "—",
                "Contrato": auto.get("contract_number") or "—",
                "Recurrence ID": auto.get("receiver_recurrence_id") or "—",
                "Invoice ID": inv.get("id"),
            }
        )

    st.dataframe(linhas, use_container_width=True, hide_index=True)

    st.divider()
    with st.expander("🔎 Inspecionar uma fatura específica"):
        invoice_id = st.text_input("Invoice ID", key="insp_id")
        if st.button("Consultar", key="insp_btn") and invoice_id.strip():
            try:
                r = consultar_fatura(parceiro["token"], invoice_id.strip())
            except requests.RequestException as e:
                st.error(f"Erro de conexão: {e}")
                return
            if r.status_code >= 400:
                st.error(f"Erro {r.status_code}")
                st.code(r.text)
                return
            inv = r.json()
            st.markdown(f"**Situação:** {classificar_fatura(inv)}")
            st.markdown(f"**Status:** {inv.get('status')}")
            st.markdown(f"**Cliente:** {inv.get('payer_name')}")
            st.markdown(f"**Valor:** {inv.get('total')}")
            st.markdown(f"**Pago em:** {inv.get('paid_at') or '—'}")
            st.json(inv.get("automatic_pix") or {})


def main():
    if not check_password():
        st.stop()

    st.title("💸 Vesti - Pix Automático")

    parceiros = carregar_parceiros()
    if not parceiros:
        st.error(
            "Nenhum parceiro configurado. Adicione os parceiros em Settings → Secrets."
        )
        st.stop()

    tab1, tab2 = st.tabs(["🚀 Gerar fatura", "📋 Conferir faturas"])
    with tab1:
        pagina_gerar(parceiros)
    with tab2:
        pagina_conferir(parceiros)


if __name__ == "__main__":
    main()
