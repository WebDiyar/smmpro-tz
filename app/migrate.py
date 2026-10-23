import asyncio
from pathlib import Path

import asyncpg

from app.config import settings

LOCK_ID = 727001


async def migrate(dsn: str, directory: Path = Path("migrations")) -> list[str]:
    """Применяет новые migrations/NNNN_*.sql по порядку; возвращает версии, применённые сейчас."""
    conn = await asyncpg.connect(dsn)
    applied: list[str] = []
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", LOCK_ID)  # параллельные запуски идут по очереди
        await conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        done = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
        for path in sorted(Path(directory).glob("*.sql")):
            if path.stem in done:
                continue
            async with conn.transaction():
                await conn.execute(path.read_text())
                await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", path.stem)
            applied.append(path.stem)
    finally:
        await conn.close()  # закрытие сессии снимает advisory lock
    return applied


if __name__ == "__main__":
    print("applied:", asyncio.run(migrate(settings.database_url)) or "nothing")
