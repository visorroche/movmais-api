
# MOVMAIS API

## Requisitos
- Node.js 18+
- PostgreSQL 13+

## 1) Instalação
```bash
npm install
```

## 2) Configurar variáveis de ambiente
Copie o arquivo de exemplo e ajuste se necessário:

```bash
cp .env.example .env
```

Variáveis principais:
- `PORT` (default: `5003`)
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`
- `JWT_SECRET`
- `CORS_ORIGINS` (default: `http://localhost:5173`)
- `CORS_HEADERS` (default inclui `Authorization` e `X-Company-Id`)

## 3) Subir o banco Postgres
Você precisa de um banco Postgres rodando e com o database configurado (ex: `movmais`).

Exemplo (via psql):
```sql
CREATE DATABASE movmais;
```

## 4) Rodar em desenvolvimento
```bash
npm run dev
```

Servidor em:
`http://localhost:5003`

## 5) Swagger
`http://localhost:5003/api-docs`

## 6) Rotas principais
- `GET /health`
- `POST /users/register`
  - body: `{ email, password, name, company_name, site }` (ou `company_site` como alias de `site`)
- `POST /users/login`
  - body: `{ email, password }`
- `GET /users/me`
  - header: `Authorization: Bearer <token>`
- `GET /companies/me`
  - header: `Authorization: Bearer <token>`
- `PUT /companies/me`
  - header: `Authorization: Bearer <token>`

## 7) Build / Produção
```bash
npm run build
npm start
```
