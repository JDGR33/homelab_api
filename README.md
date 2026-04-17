# Benni Consumer Service

Small API service that connects to Benni PostgreSQL and returns the first 5 rows from `currency_rates`.

## Endpoints

- `GET /health`
- `GET /currency-rates/first-five`

## Setup

1. Make sure Benni stack is running from the project root:

```bash
docker compose up -d --build
```

2. Prepare environment for this service:

```bash
cd other-service
cp .env.example .env
```

3. Edit `.env` values. For quick test, you can use the same DB credentials as Benni.

4. Start the consumer service:

```bash
docker compose up -d --build
```

5. Test:

```bash
curl http://localhost:8081/currency-rates/first-five
```

## Optional: Read-only DB user

Run this once in the Benni PostgreSQL container:

```bash
docker exec -it benni-postgres psql -U "$DB_USER" -d "$DB_NAME"
```

Then execute SQL:

```sql
CREATE USER benni_reader WITH PASSWORD 'change_me';
GRANT CONNECT ON DATABASE your_database TO benni_reader;
GRANT USAGE ON SCHEMA public TO benni_reader;
GRANT SELECT ON TABLE currency_rates TO benni_reader;
```

After that, set `BENNI_DB_USER=benni_reader` in `other-service/.env`.
