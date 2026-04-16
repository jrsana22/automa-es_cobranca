# Jornal de Ações - Automação Cobrança

## 2026-04-14 — Início do Projeto

### Decisões tomadas
- **Arquitetura**: HTTP direto (requests) ao invés de Playwright/Selenium — motivo: consumo de RAM/CPU insustentável com browser automation para 20+ clientes
- **Stack**: FastAPI + Jinja2 + SQLite + APScheduler + requests + Google Sheets API
- **Infra**: VPS dedicada (2-4 vCPU, 4-8GB RAM)
- **Escalabilidade**: Cada cliente = ~50MB RAM (vs ~500MB com browser)
- **Dashboard**: FastAPI + HTML simples para CRUD de clientes e visualização de logs

### Modelo de dados
- **Automacao** (não "Cliente") — cada automação é um passo a passo completo com nome, ERP, login/senha, planilha, aba, filtro, horário
- **Execucao** — log de cada execução com status, registros encontrados/filtrados, detalhes
- Suporte a dois tipos de ERP: APVS Brasil (`apvs_brasil`) e APVS Truck (`apvs_truck`)

### Estrutura criada
- `app/main.py` — FastAPI app com lifespan
- `app/config.py` — Settings via .env
- `app/database.py` — SQLAlchemy + SQLite
- `app/models.py` — Automacao + Execucao
- `app/scheduler.py` — APScheduler com cron por automação
- `app/routers/dashboard.py` — Rotas HTML
- `app/routers/api.py` — CRUD REST + criptografia de senhas
- `app/routers/executions.py` — Executar automação (individual e todos)
- `app/services/erp_client.py` — Base class para ERP clients
- `app/services/erp_apvs.py` — Client APVS Brasil (implementado e testado ✅)
- `app/services/erp_truck.py` — Client APVS Truck (placeholder, aguardando passo a passo)
- `app/services/erp_factory.py` — Factory para escolher client pelo tipo
- `app/services/sheets.py` — Google Sheets writer (OAuth, limpar, escrever)
- `app/services/processor.py` — Orquestrador: ERP → filtro D-1 → Sheets
- Templates: dashboard.html, automacao_form.html, logs.html
- Docker + docker-compose

### Próximos passos
1. ~~**Reverse-engineering do ERP APVS** — mapear chamadas HTTP com DevTools~~ ✅
2. ~~**Preencher erp_apvs.py** com endpoints reais~~ ✅
3. **Testar execução completa** (ERP → filtro D-1 → Google Sheets)
4. **Receber passo a passo da APVS Truck** e implementar erp_truck.py
5. **Deploy na VPS**

## 2026-04-14 — Reverse Engineering APVS Completo

### Fluxo de login mapeado (via DevTools no navegador)
O ERP APVS é uma SPA ASP.NET WebForms. O login requer 4 passos HTTP:
1. `GET /WebClient.aspx` → inicializa sessão + cookies
2. `POST /default.aspx/GravarVisitorID` → grava fingerprint (visitorId → cookie I4ProEngine)
3. `POST /default.aspx/GravarTimezone` → grava timezone
4. `GET /Default.aspx?` → página de login com `__RequestVerificationToken`
5. `POST /Default.aspx` com cd_usuario + nm_senha + token → sessão autenticada

### Fluxo de exportação mapeado
1. `GET /Default.aspx?eng_idtela=127000653&eng_idmenu=127000051&eng_idmodulo=127&eng_detalhe=s` → carrega formulário
2. Captura todos os campos hidden (eng_token, eng_chk, etc.)
3. `POST /Excel.aspx` com todos os campos → retorna XLSX direto

### Colunas do XLSX exportado
nome, cpf_cnpj, contrato, dt_contrato, dt_inicio_vigencia, placa, chassi, nm_forma_pagamento, parcela, vencimento_Parcela, vencimento_Orignal, valor_total, celular, fone, email, boleto, link, regional, e-mail regional, consultor, situacao, qtd_dias_inadimplencia, qtd de parcelas vencidas, dt_followup, nm_auto, status, dt_cancelamento

### Resultado do teste
- Login OK ✅
- Exportação OK ✅ (57 registros)
- Filtro D-1 OK ✅ (19 registros para 2026-04-13)