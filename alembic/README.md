# Alembic Migration Guide

## Initialize current schema baseline

```bash
alembic revision --autogenerate -m "baseline"
alembic upgrade head
```

## Create new migration

```bash
alembic revision --autogenerate -m "add_xxx"
```

## Upgrade / Downgrade

```bash
alembic upgrade head
alembic downgrade -1
```

Notes:
- `alembic.ini` leaves `sqlalchemy.url` empty by default.
- Alembic will therefore follow `APP_DATABASE_URL`, `DATABASE_URL`, or `src.config.settings.get_database_url()` in the same order as the app.
- Set `APP_DATABASE_URL` (or `DATABASE_URL`) to point at the shared PostgreSQL instance so migrations run against your target database.
