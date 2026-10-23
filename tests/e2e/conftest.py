import asyncio
import hashlib
import hmac
import json
import os
import time
import uuid
from collections.abc import Callable

import asyncpg
import httpx
import pytest_asyncio

DATABASE_URL = os.environ["DATABASE_URL"]
API_URL = os.environ.get("API_URL", "http://api:8000")
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
PROVIDER_URLS = {"alpha": os.environ["ALPHA_URL"], "beta": os.environ["BETA_URL"]}


@pytest_asyncio.fixture
async def db():
    conn = await asyncpg.connect(DATABASE_URL)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def pool():
    """Собственный пул теста для прямых вызовов watchdog.tick. Маленький: у 4 воркеров api свои пулы."""
    p = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    yield p
    await p.close()


@pytest_asyncio.fixture
async def api():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        yield client


def signed(payload: dict) -> tuple[bytes, dict]:
    """Тело и заголовки как у отправителя. HMAC считается здесь независимо от app.signing."""
    raw = json.dumps(payload).encode()
    sig = hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return raw, {"X-Signature": sig, "Content-Type": "application/json"}


async def new_user(api, tokens: int) -> int:
    """Создаёт пользователя оплатой с уникальным payment_id."""
    user_id = uuid.uuid4().int >> 66  # < 2**62, влезает в bigint
    raw, h = signed({"payment_id": uuid.uuid4().hex, "user_id": user_id, "tokens": tokens, "status": "succeeded"})
    r = await api.post("/webhooks/payments", content=raw, headers=h)
    assert r.status_code == 200 and r.json() == {"credited": True}, r.text
    return user_id


async def provider_webhook(api, name: str, **payload) -> httpx.Response:
    """Подписанный вебхук провайдера (Д7) на /webhooks/providers/{name}."""
    raw, h = signed(payload)
    return await api.post(f"/webhooks/providers/{name}", content=raw, headers=h)


async def wait_task(api, task_id, until: Callable[[dict], bool], timeout: float = 15) -> dict:
    """Опрашивает GET /tasks/{id}, пока until(task) не станет истинным; при тайм-ауте падает с последним состоянием."""
    deadline = time.monotonic() + timeout
    while True:
        r = await api.get(f"/tasks/{task_id}")
        assert r.status_code == 200, r.text
        task = r.json()
        if until(task):
            return task
        assert time.monotonic() < deadline, f"task {task_id} timed out in state {task}"
        await asyncio.sleep(0.2)


async def ledger_rows(db, user_id, reason=None) -> list:
    return await db.fetch(
        "SELECT * FROM ledger WHERE user_id = $1 AND ($2::text IS NULL OR reason = $2) ORDER BY id",
        user_id, reason,
    )


@pytest_asyncio.fixture(autouse=True)
async def balance_invariant():
    """Д1: после каждого теста баланс каждого пользователя равен сумме его журнала."""
    yield
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        bad = await conn.fetchval(
            "SELECT count(*) FROM users u WHERE u.balance <> "
            "(SELECT coalesce(sum(amount), 0) FROM ledger l WHERE l.user_id = u.id)"
        )
    finally:
        await conn.close()
    assert bad == 0, f"{bad} users have balance != sum(ledger)"


async def configure(provider: str, **cfg) -> None:
    """Режим фейкового провайдера (Д7): fail_rate, fail_type, submit_error, webhook, delay_min/max..."""
    async with httpx.AsyncClient(base_url=PROVIDER_URLS[provider], timeout=10) as c:
        r = await c.put("/_admin/config", json=cfg)
        assert r.status_code == 200, r.text


@pytest_asyncio.fixture(autouse=True)
async def quiet_fakes():
    """Каждый тест начинает с молчащих провайдеров: нужный режим он включает сам, чужие вебхуки не прилетают."""
    for name, url in PROVIDER_URLS.items():
        async with httpx.AsyncClient(base_url=url, timeout=10) as c:
            assert (await c.post("/_admin/reset")).status_code == 200
        await configure(name, webhook="never")
