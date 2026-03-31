# Hey Analytics

Aplicação FastAPI para atribuição e analytics de funil Meta Ads -> Typeform -> Monday CRM, com dashboard web em tempo real.

## 1) Configuração de variáveis de ambiente no Railway

No serviço do Railway, abra **Variables** e configure:

- `DATABASE_URL`
- `META_ACCESS_TOKEN`
- `META_AD_ACCOUNT_ID`
- `TYPEFORM_SECRET`
- `MONDAY_API_KEY`
- `MONDAY_BOARD_ID`

Dica: o PostgreSQL plugin do Railway injeta automaticamente `DATABASE_URL`.

## 2) Hidden fields UTM no Typeform

No link de divulgação do Typeform, passe os hidden fields:

- `utm_campaign`
- `utm_adset`
- `utm_ad`
- `utm_source`
- `utm_medium`
- `lp_url`

Exemplo:

```text
https://seu-form.typeform.com/to/abc123?utm_campaign={{campaign.name}}&utm_adset={{adset.name}}&utm_ad={{ad.name}}&utm_source=meta&utm_medium=cpc&lp_url=https://sualp.com
```

No Typeform, confirme que esses campos estão habilitados em **Hidden Fields** para o formulário.

## 3) Webhook do Typeform

No Typeform:

1. Abra o formulário -> **Connect** -> **Webhooks**.
2. Crie um webhook apontando para:
   - `https://sua-url.railway.app/webhook/typeform`
3. Ative assinatura e configure o mesmo valor de secret em `TYPEFORM_SECRET`.
4. Header esperado: `Typeform-Signature: sha256=HASH`.

## 4) Mapeamento de colunas no Monday CRM

No board de leads (`MONDAY_BOARD_ID`), crie colunas que representem pelo menos:

- Email do lead (id contendo `email`)
- Respondeu (id sugerido: `responded`)
- Reunião agendada (id sugerido: `meeting_scheduled`)
- Reunião realizada (id sugerido: `meeting_done`)
- Venda (id sugerido: `sale`)
- Datas opcionais:
  - `meeting_scheduled_at`
  - `meeting_done_at`
  - `sale_at`

A sincronização tenta identificar as colunas por id (heurística por nome). Use ids padronizados para maior confiabilidade.

## 5) Primeiro deploy no Railway via GitHub

1. Suba este projeto para um repositório no GitHub.
2. No Railway, clique em **New Project** -> **Deploy from GitHub Repo**.
3. Selecione o repositório.
4. Railway detectará `Dockerfile` e usará `railway.toml`.
5. Configure as variáveis de ambiente.
6. Aguarde o build e deployment.
7. Valide:
   - Healthcheck: `/health`
   - Dashboard: `/`
   - Webhook: `/webhook/typeform`

## Execução local

```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload
```

## Agendamentos

- `sync_meta_ads`: a cada 1 hora
- `sync_monday`: a cada 30 minutos

## Observações técnicas

- Banco inicializado automaticamente no startup com `CREATE TABLE IF NOT EXISTS`.
- `psycopg2` com `ThreadedConnectionPool(minconn=1, maxconn=10)`.
- Logs estruturados via `logging`.
- Divisões por zero protegidas.
- Dashboard responsivo e com auto-refresh a cada 5 minutos.
