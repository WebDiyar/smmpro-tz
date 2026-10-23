import asyncio
import time
import uuid

import asyncpg
from conftest import DATABASE_URL, configure, ledger_rows, new_user, provider_webhook, signed, wait_task
from test_generate import balance, order

FAST = {"webhook": "once", "delay_min": 0, "delay_max": 0.5}


def finished(t: dict) -> bool:
    return t["status"] != "running"


async def submit(api, body: dict) -> str:
    r = await api.post("/generate", json=body)
    assert r.status_code == 202, r.text
    return r.json()["task_id"]


def history(task: dict) -> list:
    return [(a["provider"], a["status"], a["error_type"]) for a in task["attempts"]]


async def webhooks_at_once(api, db, task_id: str, name: str, payload: dict, k: int) -> list:
    """k одинаковых вебхуков встречаются на блокировке строки задачи одновременно.

    Тест держит FOR UPDATE строки задачи, пока все k запросов (на 4 воркерах) не встанут в ожидание
    на ней же (FOR UPDATE OF t в fail_attempt), и только потом отпускает.
    """
    barrier = await asyncpg.connect(DATABASE_URL)
    try:
        async with barrier.transaction():
            await barrier.execute("SELECT 1 FROM tasks WHERE id = $1 FOR UPDATE", uuid.UUID(task_id))
            sends = asyncio.gather(*[provider_webhook(api, name, **payload) for _ in range(k)])
            waiting = 0
            for _ in range(300):
                waiting = await db.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                    "AND query LIKE '%FOR UPDATE OF t%'"
                )
                if waiting == k:
                    break
                await asyncio.sleep(0.05)
            assert waiting == k, f"only {waiting} webhooks reached the task lock"
    finally:
        await barrier.close()
    return await sends


async def test_first_leg_fails_second_completes(api, db):
    """Обязательный №3: первая нога отказала, задача завершилась на второй."""
    await configure("alpha", fail_rate=1, fail_type="technical", **FAST)
    await configure("beta", **FAST)
    uid = await new_user(api, 10)
    task = await wait_task(api, await submit(api, order(uid)), finished)
    assert task["status"] == "completed", task
    assert history(task) == [("alpha", "failed", "technical"), ("beta", "completed", None)]
    assert task["result_url"]
    assert await balance(api, uid) == 8
    assert await ledger_rows(db, uid, "refund") == []


async def test_ten_concurrent_failed_on_last_leg_refund_once(api, db):
    """Обязательный №2: 10 одновременных failed на последней ноге - ровно один возврат."""
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid, refs=5))  # единственная подходящая нога - beta, она молчит
    task = await wait_task(api, task_id, lambda t: t["attempts"][0]["status"] == "pending")
    [a] = task["attempts"]
    assert a["provider"] == "beta"
    payload = {"reference": a["id"], "external_id": a["external_id"], "status": "failed", "error_type": "technical"}
    responses = await webhooks_at_once(api, db, task_id, "beta", payload, 10)
    assert [r.status_code for r in responses] == [200] * 10, [r.text for r in responses]
    task = await wait_task(api, task_id, finished, timeout=1)
    assert task["status"] == "failed"
    assert history(task) == [("beta", "failed", "technical")]
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [2]
    assert await balance(api, uid) == 10


async def test_payload_fails_immediately_no_next_leg(api, db):
    await configure("alpha", fail_rate=1, fail_type="payload", **FAST)
    await configure("beta", **FAST)
    uid = await new_user(api, 10)
    task = await wait_task(api, await submit(api, order(uid)), finished)
    assert task["status"] == "failed" and task["result_url"] is None
    assert history(task) == [("alpha", "failed", "payload")]
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [2]
    assert await balance(api, uid) == 10


async def test_moderation_skips_flagged_leg(api, db):
    await configure("beta", fail_rate=1, fail_type="moderation", **FAST)
    await configure("alpha", **FAST)
    uid = await new_user(api, 25)
    task = await wait_task(api, await submit(api, order(uid, "video-long")), finished)
    assert task["status"] == "failed"
    assert history(task) == [("beta", "failed", "moderation")]  # alpha со skip_on_moderation пропущена
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [25]
    assert await balance(api, uid) == 25


async def test_technical_does_not_skip_flagged_leg(api, db):
    await configure("beta", fail_rate=1, fail_type="technical", **FAST)
    await configure("alpha", **FAST)
    uid = await new_user(api, 25)
    task = await wait_task(api, await submit(api, order(uid, "video-long")), finished)
    assert task["status"] == "completed", task
    assert history(task) == [("beta", "failed", "technical"), ("alpha", "completed", None)]
    assert await ledger_rows(db, uid, "refund") == []
    assert await balance(api, uid) == 0


async def test_ref_count_limits_chain(api, db):
    await configure("beta", fail_rate=1, fail_type="technical", **FAST)
    await configure("alpha", **FAST)
    uid = await new_user(api, 25)
    task = await wait_task(api, await submit(api, order(uid, "video-long", refs=2)), finished)
    assert task["status"] == "failed"
    assert history(task) == [("beta", "failed", "technical")]  # alpha берёт только 1 референс
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [25]


async def test_all_legs_fail_refund_once(api, db):
    await configure("alpha", fail_rate=1, fail_type="technical", **FAST)
    await configure("beta", fail_rate=1, fail_type="technical", **FAST)
    uid = await new_user(api, 10)
    task = await wait_task(api, await submit(api, order(uid)), finished)
    assert task["status"] == "failed"
    assert history(task) == [("alpha", "failed", "technical"), ("beta", "failed", "technical")]
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [2]
    assert await balance(api, uid) == 10


async def test_sync_503_moves_to_next_leg(api):
    await configure("alpha", submit_error="503", **FAST)
    await configure("beta", **FAST)
    uid = await new_user(api, 10)
    task = await wait_task(api, await submit(api, order(uid)), finished)
    assert task["status"] == "completed", task
    assert history(task) == [("alpha", "failed", "submit_error"), ("beta", "completed", None)]


async def test_sync_timeout_moves_to_next_leg(api):
    await configure("alpha", submit_error="timeout", submit_sleep=10, webhook="never")
    await configure("beta", **FAST)
    uid = await new_user(api, 10)
    started = time.monotonic()
    task = await wait_task(api, await submit(api, order(uid)), finished, timeout=15)
    assert time.monotonic() - started < 15
    assert task["status"] == "completed", task
    assert task["attempts"][0]["error_type"] == "submit_error"
    assert history(task) == [("alpha", "failed", "submit_error"), ("beta", "completed", None)]


async def test_provider_duplicate_concurrent_webhooks(api, db):
    await configure("alpha", **(FAST | {"webhook": "twice_concurrent"}))
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    task = await wait_task(api, task_id, finished)
    assert task["status"] == "completed" and history(task) == [("alpha", "completed", None)]
    await asyncio.sleep(1)
    assert (await api.get(f"/tasks/{task_id}")).json() == task
    assert await balance(api, uid) == 8


async def test_webhook_before_dispatch_recorded(api):
    """Review Focus 1: вебхук может обогнать ответ 202 - результат не теряется."""
    await configure("alpha", webhook="once", delay_min=0, delay_max=0)
    uid = await new_user(api, 10)
    task = await wait_task(api, await submit(api, order(uid)), finished)
    assert task["status"] == "completed" and task["result_url"]
    assert task["attempts"][0]["status"] == "completed"
    assert task["attempts"][0]["external_id"]


async def test_webhook_wrong_provider_or_reference_404(api):
    """Review Focus 4: чужой reference, чужой провайдер, неизвестный провайдер - 404, состояние не меняется."""
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    before = await wait_task(api, task_id, lambda t: t["attempts"][0]["status"] == "pending")
    a = before["attempts"][0]
    ok = {"external_id": "x", "status": "completed", "result_url": "https://files.example/x.png"}
    assert (await provider_webhook(api, "beta", reference=a["id"], **ok)).status_code == 404
    assert (await provider_webhook(api, "alpha", reference=str(uuid.uuid4()), **ok)).status_code == 404
    assert (await provider_webhook(api, "gamma", reference=a["id"], **ok)).status_code == 404
    assert (await api.get(f"/tasks/{task_id}")).json() == before


async def test_provider_webhook_bad_signature_401(api):
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    before = await wait_task(api, task_id, lambda t: t["attempts"][0]["status"] == "pending")
    raw, h = signed({"reference": before["attempts"][0]["id"], "external_id": "x",
                     "status": "failed", "error_type": "technical"})
    for headers in ({**h, "X-Signature": "0" * 64}, {"Content-Type": "application/json"}):
        r = await api.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert r.status_code == 401, r.text
    assert (await api.get(f"/tasks/{task_id}")).json() == before


async def test_provider_webhook_shape_422(api):
    ref = str(uuid.uuid4())
    for bad in ({"status": "completed"}, {"status": "failed"}, {"status": "failed", "error_type": "timeout"},
                {"status": "lost", "error_type": "technical"}):
        r = await provider_webhook(api, "alpha", reference=ref, external_id="x", **bad)
        assert r.status_code == 422, (bad, r.text)
