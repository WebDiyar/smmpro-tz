"""Сторож (Д4). Работает в каждом из 4 процессов, лидера нет.

FOR UPDATE OF t SKIP LOCKED раздаёт разным процессам разные просроченные задачи, а fail_attempt
после блокировки перепроверяет состояние - если вебхук или другой сторож успел раньше, шаг - no-op.
Дедлайн хранится в БД и сравнивается с now() БД: часы процессов не участвуют.
"""
import asyncio
import logging
from uuid import UUID

from app import orchestrator

log = logging.getLogger("app.watchdog")

MAX_STEPS = 100  # шагов за тик: остальное заберёт следующий тик или другой процесс

OVERDUE = (
    "SELECT a.id FROM tasks t JOIN attempts a ON a.id = t.current_attempt_id "
    "WHERE t.status = 'running' AND a.status IN ('dispatching', 'pending') AND a.deadline_at < now() "
    "AND a.id <> ALL($1::uuid[]) "
    "ORDER BY a.deadline_at LIMIT 1 "
    "FOR UPDATE OF t SKIP LOCKED"
)


async def tick(pool) -> list[UUID]:
    """Снимает просроченные попытки, по одной на транзакцию. Возвращает id новых попыток для отправки после COMMIT.

    Сбой шага не должен терять уже закоммиченные новые попытки и не должен блокировать остальные задачи:
    упавшая попытка попадает в skip до конца тика, а new_ids возвращается всегда.
    """
    new_ids, skip = [], []
    for _ in range(MAX_STEPS):
        attempt_id = None
        try:
            async with pool.acquire() as conn, conn.transaction():
                attempt_id = await conn.fetchval(OVERDUE, skip)
                if attempt_id is None:
                    break
                new_id = await orchestrator.fail_attempt(conn, attempt_id, "timeout")
        except Exception:
            log.exception("watchdog step failed (attempt %s)", attempt_id)
            if attempt_id is None:  # упал сам SELECT: дальше идти некуда
                break
            skip.append(attempt_id)
            continue
        if new_id is not None:
            new_ids.append(new_id)
    return new_ids


async def run(pool, http, interval: float) -> None:
    """Бесконечный цикл из lifespan. Ошибка тика логируется, цикл не умирает; отмена - только при остановке."""
    while True:
        try:
            new_ids = await tick(pool)
            for r in await asyncio.gather(*[orchestrator.dispatch(pool, http, i) for i in new_ids],
                                          return_exceptions=True):
                if isinstance(r, Exception):
                    log.error("watchdog dispatch failed", exc_info=r)
        except Exception:
            log.exception("watchdog tick failed")
        await asyncio.sleep(interval)
