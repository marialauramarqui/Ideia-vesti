# Setup do upload de planilha do Diogo (Cloudflare Worker)

Pra deixar o botao "Subir planilha" do dashboard funcionando, voce precisa fazer
3 coisas (uma vez, ~10 min):

1. Criar um Personal Access Token no GitHub.
2. Criar o Worker no Cloudflare e colar o `worker.js`.
3. Colar a URL do Worker no dashboard.

---

## 1. GitHub: criar fine-grained PAT

1. Vai em https://github.com/settings/personal-access-tokens/new
2. **Token name**: `onlog-uploader`
3. **Expiration**: 1 ano (ou "No expiration").
4. **Resource owner**: `vesti-mobi`
5. **Repository access**: Only select repositories -> `vesti-mobi/Ideia-vesti`
6. **Permissions** (Repository permissions):
   - Contents: **Read and write**
   - Actions: **Read and write**
   - Metadata: Read-only (auto)
7. Generate token e **copia o valor** (so aparece uma vez).

> Se o repo `vesti-mobi/Ideia-vesti` esta numa organizacao, talvez precise
> aprovar o token em Organization > Settings > Personal access tokens > Pending requests.

---

## 2. Cloudflare: criar Worker

1. Conta gratis em https://dash.cloudflare.com (se ja tiver, faz login).
2. Workers & Pages > Create > Create Worker.
3. Nome sugerido: `onlog-uploader`. Deploy.
4. Edit code -> apaga o conteudo padrao -> cola o `worker.js` deste diretorio.
5. **Deploy**.

### Configurar variaveis (Worker > Settings > Variables and Secrets):

| Tipo   | Nome              | Valor                                                         |
|--------|-------------------|---------------------------------------------------------------|
| Secret | `GITHUB_TOKEN`    | (cola o PAT do passo 1)                                       |
| Secret | `UPLOAD_PASSWORD` | escolhe uma senha simples (ex: `vestionlog2026`)              |
| Text   | `GITHUB_OWNER`    | `vesti-mobi`                                                  |
| Text   | `GITHUB_REPO`     | `Ideia-vesti`                                                 |
| Text   | `WORKFLOW_FILE`   | `onlog-ingest.yml`                                            |

Salva. O Worker ja esta no ar com URL tipo
`https://onlog-uploader.<seu-subdominio>.workers.dev`.

### Testar (opcional)

```bash
curl https://onlog-uploader.<subdominio>.workers.dev/status
```
Tem que retornar `{"runs":[]}` (ou os runs anteriores).

---

## 3. Plugar no dashboard

No `template.html` (ou no `index.html` direto), procura a constante `ONLOG_UPLOAD_WORKER_URL`
e troca pela URL do seu Worker. Depois roda:

```
py build_html.py
git add PainelCSGerencial/template.html PainelCSGerencial/index.html
git commit -m "Onlog: configurar URL do worker"
git push
```

Pronto. Qualquer pessoa que souber a senha pode subir a planilha pelo botao
"Subir planilha do Diogo" na aba Frete Onlog do dashboard.

---

## Como funciona o fluxo

1. Usuaria clica "Subir planilha", escolhe `.xlsx`, digita senha.
2. JS converte pra base64, manda POST pro Worker.
3. Worker valida senha, commita o arquivo em `PainelCSGerencial/_uploads/<timestamp>_<nome>.xlsx`.
4. Worker dispara o GitHub Action `onlog-ingest.yml`.
5. Action roda `ingest_diogo_onlog.py` -> `merge_data.py` -> `build_html.py`,
   commita `index.html` atualizado e republica em `marialauramarqui/PainelCSGerencial`.
6. GitHub Pages reconstroi (~1 min). Total: 2-3 min.
