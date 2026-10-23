import asyncio
import uuid

import asyncpg
from conftest import DATABASE_URL, ledger_rows, new_user, wait_task


def order(user_id: int, model_slug: str = "image-fast", refs: int = 0) -> dict:
    return {"user_id": user_id, "model_slug": model_slug, "prompt": "a cat",
            "ref_images": [f"https://img.example/{i}.png" for i in range(refs)]}


async def balance(api, user_id: int) -> int:
    r = await api.get(f"/users/{user_id}/balance")
    assert r.status_code == 200, r.text
    return r.json()["balance"]


async def generate_at_once(api, db, bodies: list[dict]) -> list:
    """Все запросы встречаются в БД одновременно (тот же приём, что в test_payments).

    SHARE-блокировка tasks задерживает INSERT INTO tasks. Если /generate держит блокировку строки
    пользователя, первый запрос ждёт на барьере, а остальные - на строке пользователя; без неё все
    проходят проверки и ждут на барьере. В обоих случаях барьер отпускается, только когда в очереди
    стоят все запросы.
    """
    barrier = await asyncpg.connect(DATABASE_URL)
    try:
        async with barrier.transaction():
            await barrier.execute("LOCK TABLE tasks IN SHARE MODE")
            sends = asyncio.gather(*[api.post("/generate", json=b) for b in bodies])
            waiting = 0
            for _ in range(300):
                waiting = await db.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                    "AND (query LIKE 'INSERT INTO tasks%' OR query LIKE 'SELECT balance FROM users%')"
                )
                if waiting == len(bodies):
                    break
                await asyncio.sleep(0.05)
            assert waiting == len(bodies), f"only {waiting} requests reached the barrier"
    finally:
        await barrier.close()
    return await sends


async def test_generate_charges_and_dispatches_first_leg(api, db):
    uid = await new_user(api, 10)
    r = await api.post("/generate", json=order(uid))
    assert r.status_code == 202, r.text
    task_id = r.json()["task_id"]
    assert await balance(api, uid) == 8
    charges = await ledger_rows(db, uid, "charge")
    assert [(c["amount"], str(c["task_id"])) for c in charges] == [(-2, task_id)]

    task = await wait_task(api, task_id, lambda t: t["attempts"][0]["status"] == "pending")
    assert task["status"] == "running" and task["model_slug"] == "image-fast"
    [a] = task["attempts"]
    assert a["n"] == 1 and a["provider"] == "alpha" and a["external_id"]
    assert task["current_leg"] == {"provider": "alpha", "priority": 1, "attempt_id": a["id"]}


async def test_ref_count_picks_first_eligible_leg(api):
    uid = await new_user(api, 10)
    r = await api.post("/generate", json=order(uid, refs=5))  # alpha берёт до 4
    assert r.status_code == 202, r.text
    task = await wait_task(api, r.json()["task_id"], lambda t: t["attempts"][0]["status"] == "pending")
    assert task["attempts"][0]["provider"] == "beta"
    assert task["current_leg"]["provider"] == "beta" and task["current_leg"]["priority"] == 2


async def test_no_eligible_leg_400_before_charge(api, db):
    uid = await new_user(api, 100)
    for body in (order(uid, refs=11), order(uid, "video-long", refs=3)):
        r = await api.post("/generate", json=body)
        assert r.status_code == 400, r.text
    assert await balance(api, uid) == 100
    assert await ledger_rows(db, uid, "charge") == []
    assert await db.fetchval("SELECT count(*) FROM tasks WHERE user_id = $1", uid) == 0


async def test_unknown_model_400_unknown_user_404(api):
    uid = await new_user(api, 10)
    assert (await api.post("/generate", json=order(uid, "no-such-model"))).status_code == 400
    stranger = uuid.uuid4().int >> 66
    assert (await api.post("/generate", json=order(stranger))).status_code == 404
    # 400 решается до пользователя (Д2)
    assert (await api.post("/generate", json=order(stranger, refs=11))).status_code == 400


async def test_concurrent_generate_respects_active_limit(api, db):
    uid = await new_user(api, 100)
    responses = await generate_at_once(api, db, [order(uid)] * 6)
    assert sorted(r.status_code for r in responses) == [202] * 3 + [429] * 3
    assert len(await ledger_rows(db, uid, "charge")) == 3
    assert await balance(api, uid) == 94


async def test_concurrent_generate_respects_balance(api, db):
    uid = await new_user(api, 3)
    responses = await generate_at_once(api, db, [order(uid)] * 2)
    assert sorted(r.status_code for r in responses) == [202, 402]
    assert await balance(api, uid) == 1
    assert len(await ledger_rows(db, uid, "charge")) == 1


async def test_get_task_404(api):
    assert (await api.get(f"/tasks/{uuid.uuid4()}")).status_code == 404
