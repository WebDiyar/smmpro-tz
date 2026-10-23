"""Задачи и попытки (Д2, Д3). Все переходы - в БД; HTTP к провайдеру - только после COMMIT."""
import logging
from uuid import UUID

import httpx

from app import money
from app.config import settings

log = logging.getLogger("app.orchestrator")

MAX_ACTIVE_TASKS = 3  # Д2: константа, а не настройка


class AdmissionError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


async def create_task(pool, user_id: int, model_slug: str, prompt: str, ref_images: list[str]) -> tuple[UUID, UUID]:
    """Приём заказа. Порядок проверок (Д2): модель 400 -> нет ноги 400 -> пользователь 404 -> 429 -> 402.

    Каталог читается до транзакции: 400 не берёт блокировок и ничего не списывает. Проверки лимита и
    баланса, задача, списание и первая попытка - одна транзакция под FOR NO KEY UPDATE строки пользователя,
    поэтому параллельные /generate одного пользователя на любых воркерах проходят по очереди.
    """
    async with pool.acquire() as conn:
        price = await conn.fetchval("SELECT price FROM models WHERE slug = $1", model_slug)
        if price is None:
            raise AdmissionError(400, "unknown model")
        legs = await conn.fetch(
            "SELECT id, provider, max_ref_images FROM legs WHERE model_slug = $1 ORDER BY priority", model_slug
        )
        leg = next((x for x in legs if x["max_ref_images"] >= len(ref_images)), None)  # допущение 1
        if leg is None:
            raise AdmissionError(400, "no provider leg accepts this many reference images")

        async with conn.transaction():
            balance = await conn.fetchval("SELECT balance FROM users WHERE id = $1 FOR NO KEY UPDATE", user_id)
            if balance is None:
                raise AdmissionError(404, "user not found")
            active = await conn.fetchval(
                "SELECT count(*) FROM tasks WHERE user_id = $1 AND status = 'running'", user_id
            )
            if active >= MAX_ACTIVE_TASKS:
                raise AdmissionError(429, f"at most {MAX_ACTIVE_TASKS} active tasks")
            if balance < price:
                raise AdmissionError(402, "insufficient balance")
            task_id = await conn.fetchval(
                "INSERT INTO tasks (user_id, model_slug, prompt, ref_images) VALUES ($1, $2, $3, $4) RETURNING id",
                user_id, model_slug, prompt, ref_images,
            )
            await conn.execute(
                "INSERT INTO ledger (user_id, amount, reason, task_id) VALUES ($1, $2, 'charge', $3)",
                user_id, -price, task_id,
            )
            attempt_id = await conn.fetchval(
                "INSERT INTO attempts (task_id, n, leg_id, provider, status, deadline_at) "
                "VALUES ($1, 1, $2, $3, 'dispatching', now() + make_interval(secs => $4)) RETURNING id",
                task_id, leg["id"], leg["provider"], settings.provider_timeout,
            )
            await conn.execute("UPDATE tasks SET current_attempt_id = $2 WHERE id = $1", task_id, attempt_id)
    return task_id, attempt_id


async def _lock_task(conn, attempt_id: UUID):
    """Блокировка строки задачи попытки. Порядок блокировок везде один: задача -> попытка -> пользователь."""
    return await conn.fetchrow(
        "SELECT t.id, t.status, t.current_attempt_id, l.priority FROM tasks t "
        "JOIN attempts a ON a.task_id = t.id JOIN legs l ON l.id = a.leg_id "
        "WHERE a.id = $1 FOR UPDATE OF t",
        attempt_id,
    )


async def finish_failed(conn, task_id: UUID) -> None:
    """running -> failed и возврат - только если переход случился именно здесь (Д1)."""
    if await conn.fetchval(
        "UPDATE tasks SET status = 'failed', finished_at = now() WHERE id = $1 AND status = 'running' RETURNING id",
        task_id,
    ):
        await money.refund_task(conn, task_id)


async def fail_attempt(conn, attempt_id: UUID, error_type: str) -> UUID | None:
    """Единый переход для failed-вебхука, синхронной ошибки отправки и сторожа (Д3). Внутри транзакции вызывающего.

    Возвращает id следующей попытки (status dispatching) - вызывающий отправляет её после COMMIT.
    None: дубль, устаревшая попытка, или задача завершена (с возвратом).
    """
    t = await _lock_task(conn, attempt_id)
    # Перепроверки строго после блокировки: дубль или устаревший вебхук - no-op.
    if t is None or t["status"] != "running" or t["current_attempt_id"] != attempt_id:
        return None
    if not await conn.fetchval(
        "UPDATE attempts SET status = CASE WHEN $2 = 'timeout' THEN 'timed_out' ELSE 'failed' END, "
        "error_type = $2, finished_at = now() WHERE id = $1 AND status IN ('dispatching', 'pending') RETURNING id",
        attempt_id, error_type,
    ):
        return None
    if error_type == "payload":  # правило 2: любая нога отвергнет тот же запрос
        await finish_failed(conn, t["id"])
        return None
    leg = await conn.fetchrow(
        "SELECT l.* FROM legs l JOIN tasks t ON t.model_slug = l.model_slug "
        "WHERE t.id = $1 AND l.priority > $2 "
        "AND l.max_ref_images >= cardinality(t.ref_images) "
        "AND NOT (l.skip_on_moderation AND EXISTS ("
        "SELECT 1 FROM attempts WHERE task_id = $1 AND error_type = 'moderation')) "
        "ORDER BY l.priority LIMIT 1",
        t["id"], t["priority"],
    )
    if leg is None:  # правило 4
        await finish_failed(conn, t["id"])
        return None
    new_id = await conn.fetchval(
        "INSERT INTO attempts (task_id, n, leg_id, provider, status, deadline_at) "
        "SELECT $1, max(n) + 1, $2, $3, 'dispatching', now() + make_interval(secs => $4) "
        "FROM attempts WHERE task_id = $1 RETURNING id",
        t["id"], leg["id"], leg["provider"], settings.provider_timeout,
    )
    await conn.execute("UPDATE tasks SET current_attempt_id = $2 WHERE id = $1", t["id"], new_id)
    return new_id


async def complete_attempt(conn, attempt_id: UUID, result_url: str) -> None:
    """completed-вебхук (Д3, Д5). Пока задача running - результат принимается от любой её попытки,
    в том числе устаревшей. Если задача уже терминальна - только след в истории, деньги не трогаем."""
    t = await _lock_task(conn, attempt_id)
    if t is None:
        return
    if t["status"] == "running":
        await conn.execute(
            "UPDATE tasks SET status = 'completed', result_url = $2, finished_at = now() WHERE id = $1",
            t["id"], result_url,
        )
        await conn.execute(
            "UPDATE attempts SET status = 'completed', result_url = $2, finished_at = now() WHERE id = $1",
            attempt_id, result_url,
        )
        await conn.execute(
            "UPDATE attempts SET status = 'abandoned', finished_at = now() "
            "WHERE task_id = $1 AND id <> $2 AND status IN ('dispatching', 'pending')",
            t["id"], attempt_id,
        )
        return
    await conn.execute(
        "UPDATE attempts SET status = 'late_completed', result_url = $2, finished_at = coalesce(finished_at, now()) "
        "WHERE id = $1 AND status NOT IN ('completed', 'late_completed')",
        attempt_id, result_url,
    )


async def apply_provider_webhook(pool, provider: str, payload) -> UUID | None:
    """Вебхук провайдера - одна транзакция. Попытка ищется по нашему reference, а не по external_id:
    вебхук мог обогнать ответ 202 (Review Focus 1). Чужой reference -> LookupError (404).

    external_id пишется после перехода: переход сначала блокирует задачу, и порядок блокировок
    (задача -> попытка) совпадает со сторожем - без взаимных блокировок.
    """
    async with pool.acquire() as conn, conn.transaction():
        if not await conn.fetchval(
            "SELECT 1 FROM attempts WHERE id = $1 AND provider = $2", payload.reference, provider
        ):
            raise LookupError("attempt not found for this provider")
        if payload.status == "completed":
            new_id = None
            await complete_attempt(conn, payload.reference, payload.result_url)
        else:
            new_id = await fail_attempt(conn, payload.reference, payload.error_type)
        await conn.execute(
            "UPDATE attempts SET external_id = coalesce(external_id, $2) WHERE id = $1",
            payload.reference, payload.external_id,
        )
    return new_id


def _external_id(r: httpx.Response) -> str | None:
    """Только 202 с JSON-объектом и непустой строкой external_id считается принятой отправкой (допущение 10)."""
    if r.status_code != 202:
        return None
    try:
        ext = r.json().get("external_id")
    except (ValueError, AttributeError):
        return None
    return ext if isinstance(ext, str) and ext else None


async def dispatch(pool, http: httpx.AsyncClient, attempt_id: UUID | None) -> None:
    """Отправка попытки провайдеру (Д3). Вызывать после COMMIT: блокировки по сети не держим.

    Условный UPDATE: если вебхук или сторож успели раньше, затронуто 0 строк - так и надо.
    Любая ошибка отправки -> fail_attempt('submit_error') и сразу следующая нога (правило 3).
    """
    while attempt_id is not None:
        a = await pool.fetchrow(
            "SELECT a.id, a.provider, t.prompt, t.ref_images FROM attempts a JOIN tasks t ON t.id = a.task_id "
            "WHERE a.id = $1 AND a.status = 'dispatching'",
            attempt_id,
        )
        if a is None:
            return
        body = {
            "reference": str(a["id"]),
            "prompt": a["prompt"],
            "ref_images": a["ref_images"],
            "callback_url": f"{settings.public_url}/webhooks/providers/{a['provider']}",
        }
        try:
            r = await http.post(f"{settings.provider_urls[a['provider']]}/tasks", json=body,
                                timeout=settings.provider_http_timeout)
            external_id = _external_id(r)
            if external_id is None:
                log.warning("submit of attempt %s to %s rejected: %s %.200s",
                            attempt_id, a["provider"], r.status_code, r.text)
        except httpx.HTTPError as e:
            external_id = None
            log.warning("submit of attempt %s to %s failed: %r", attempt_id, a["provider"], e)
        if external_id is not None:
            await pool.execute(
                "UPDATE attempts SET status = 'pending', external_id = coalesce(external_id, $2) "
                "WHERE id = $1 AND status = 'dispatching'",
                attempt_id, external_id,
            )
            return
        async with pool.acquire() as conn, conn.transaction():
            attempt_id = await fail_attempt(conn, attempt_id, "submit_error")


async def get_task(pool, task_id: UUID) -> dict | None:
    """Форма из Д10. repeatable_read: задача и попытки из одного снимка."""
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        t = await conn.fetchrow(
            "SELECT t.id, t.status, t.model_slug, t.result_url, a.provider, l.priority, a.id AS attempt_id "
            "FROM tasks t LEFT JOIN attempts a ON a.id = t.current_attempt_id LEFT JOIN legs l ON l.id = a.leg_id "
            "WHERE t.id = $1",
            task_id,
        )
        if t is None:
            return None
        attempts = await conn.fetch(
            "SELECT id, n, provider, status, error_type, external_id, created_at, finished_at "
            "FROM attempts WHERE task_id = $1 ORDER BY n",
            task_id,
        )
    current_leg = None
    if t["attempt_id"] is not None:
        current_leg = {"provider": t["provider"], "priority": t["priority"], "attempt_id": t["attempt_id"]}
    return {
        "id": t["id"], "status": t["status"], "model_slug": t["model_slug"], "result_url": t["result_url"],
        "current_leg": current_leg, "attempts": [dict(r) for r in attempts],
    }
