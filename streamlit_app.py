from datetime import date, timedelta

import requests
import streamlit as st

BASE_URL = "https://api.iugu.com/v1"

st.set_page_config(page_title="Vesti - Pix Automático", page_icon="💸", layout="centered")


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
    if dados.get("enviar_payable_with"):
        payload["payable_with"] = ["pix"]

    r = requests.post(
        f"{BASE_URL}/invoices",
        auth=(token, ""),
        json=payload,
        timeout=30,
    )
    return r, payload


def main():
    if not check_password():
        st.stop()

    st.title("💸 Gerador de Pix Automático")
    st.caption("Crie cobranças recorrentes com Pix Automático da iugu")

    parceiros = carregar_parceiros()
    if not parceiros:
        st.error(
            "Nenhum parceiro configurado. Adicione os parceiros em Settings → Secrets."
        )
        st.stop()

    nomes = [p["nome"] for p in parceiros]
    parceiro_nome = st.selectbox("Parceiro (conta iugu)", nomes)
    parceiro = next(p for p in parceiros if p["nome"] == parceiro_nome)

    st.divider()
    st.subheader("Dados do cliente")

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
        enviar_payable_with = False

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
        "enviar_payable_with": enviar_payable_with,
    }

    with st.spinner(f"Criando fatura em {parceiro['nome']}..."):
        try:
            r, payload_enviado = criar_fatura(parceiro["token"], dados)
        except requests.RequestException as e:
            st.error(f"Erro de conexão: {e}")
            return

    with st.expander("📤 Payload enviado para iugu (debug)"):
        st.json(payload_enviado)

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

    auto_qr_img = (
        auto.get("qrcode")
        or auto.get("qr_code")
        or auto.get("qrcode_url")
        or auto.get("authorization_qrcode")
    )
    auto_qr_text = (
        auto.get("qrcode_text")
        or auto.get("qr_code_text")
        or auto.get("emv")
        or auto.get("authorization_qrcode_text")
        or auto.get("copy_paste")
    )
    auto_url = auto.get("authorization_url") or auto.get("url")

    st.divider()
    st.subheader("🔁 Pix Automático — pagamento + recorrência")
    st.caption(
        "Ao escanear este QR Code, o cliente paga a 1ª parcela e já autoriza "
        "automaticamente a recorrência no mesmo ato."
    )
    if auto_qr_img or auto_qr_text or auto_url:
        if auto_qr_img:
            st.image(auto_qr_img, caption="QR Code - Pix Automático (pagamento + recorrência)", width=260)
        if auto_qr_text:
            st.markdown("**Código copia e cola:**")
            st.code(auto_qr_text, language=None)
        if auto_url:
            st.link_button("🔗 Abrir link de pagamento", auto_url)
    else:
        st.warning(
            "⚠️ Nenhum QR Code de Pix Automático foi retornado pela iugu. "
            "Veja abaixo o conteúdo do objeto `automatic_pix`."
        )
        st.json(auto)

    if data.get("secure_url"):
        st.link_button("🔗 Abrir página de pagamento iugu", data["secure_url"])

    with st.expander("🧪 Ver resposta completa da iugu (debug)"):
        st.json(data)


if __name__ == "__main__":
    main()
