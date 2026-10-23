import asyncio
import json
import uuid

import asyncpg
from conftest import DATABASE_URL, WEBHOOK_SECRET, ledger_rows, new_user, signed

from app.money import refund_task
from app.signing import sign, verify


def payment(user_id: int, tokens: int = 100, status: str = "succeeded", payment_id: str | None = None) -> dict:
    return {"payment_id": payment_id or uuid.uuid4().hex, "user_id": user_id, "tokens": tokens, "status": status}


def fresh_id() -> int:
    return uuid.uuid4().int >> 66


async def balance(api, user_id: int) -> int:
    r = await api.get(f"/users/{user_id}/balance")
    assert r.status_code == 200, r.text
    return r.json()["balance"]


async def test_twenty_concurrent_payment_webhooks_credit_once(api, db):
    """Обязательный №1: 20 одновременных одинаковых доставок через 4 воркера -> одно начисление.

    Без барьера запросы на Docker Desktop растягиваются на сотни мс, первый успевает закоммититься
    раньше, чем остальные дойдут до БД, и гонка не проверяется. Барьер: SHARE-блокировка журнала
    (чтение не блокирует, INSERT ждёт) держит все 20 вставок, пока они не встанут в очередь, затем
    отпускает разом - они встречаются на уникальном индексе payment_id.
    Пользователь создан заранее: для нового пользователя дубли выстроились бы в очередь ещё на
    INSERT INTO users, и до гонки на журнале дело бы не дошло.
    """
    p = payment(fresh_id())
    raw, h = signed(p)
    await db.execute("INSERT INTO users (id) VALUES ($1)", p["user_id"])
    barrier = await asyncpg.connect(DATABASE_URL)
    try:
        async with barrier.transaction():
            await barrier.execute("LOCK TABLE ledger IN SHARE MODE")
            sends = asyncio.gather(*[api.post("/webhooks/payments", content=raw, headers=h) for _ in range(20)])
            waiting = 0
            for _ in range(300):
                waiting = await db.fetchval(
                    # только начисления: возврат, который сторож делает по чужой зависшей задаче, тоже ждёт на барьере
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                    "AND query LIKE 'INSERT INTO ledger (user_id, amount, reason, payment_id)%'"
                )
                if waiting == 20:
                    break
                await asyncio.sleep(0.05)
            assert waiting == 20, f"only {waiting} inserts reached the barrier"
    finally:
        await barrier.close()
    responses = await sends
    assert [r.status_code for r in responses] == [200] * 20
    assert sorted(r.json()["credited"] for r in responses) == [False] * 19 + [True]
    assert await balance(api, p["user_id"]) == 100
    rows = await db.fetch("SELECT * FROM ledger WHERE payment_id = $1", p["payment_id"])
    assert len(rows) == 1 and rows[0]["amount"] == 100


async def test_concurrent_first_payments_for_new_user(api, db):
    """Гонка первой оплаты: пользователя ещё нет, а 20 разных оплат и 20 дублей одной приходят разом.

    Все ветвятся на INSERT INTO users ... ON CONFLICT DO NOTHING; победитель создаёт строку, остальные
    ждут его коммита. Ни одна доставка не должна упасть 500 и ни одна оплата не должна потеряться или задвоиться.
    """
    distinct_user = fresh_id()
    sends = [signed(payment(distinct_user, tokens=5)) for _ in range(20)]
    responses = await asyncio.gather(*[api.post("/webhooks/payments", content=raw, headers=h) for raw, h in sends])
    assert [r.status_code for r in responses] == [200] * 20
    assert [r.json() for r in responses] == [{"credited": True}] * 20
    assert await balance(api, distinct_user) == 100
    assert len(await ledger_rows(db, distinct_user, "payment")) == 20

    dup = payment(fresh_id(), tokens=7)
    raw, h = signed(dup)
    responses = await asyncio.gather(*[api.post("/webhooks/payments", content=raw, headers=h) for _ in range(20)])
    assert [r.status_code for r in responses] == [200] * 20
    assert sorted(r.json()["credited"] for r in responses) == [False] * 19 + [True]
    assert await balance(api, dup["user_id"]) == 7
    assert len(await ledger_rows(db, dup["user_id"])) == 1


async def test_same_payment_id_different_body_credits_once(api, db):
    p = payment(fresh_id())
    raw, h = signed(p)
    assert (await api.post("/webhooks/payments", content=raw, headers=h)).json() == {"credited": True}
    other = fresh_id()
    for dup in ({**p, "tokens": 999}, {**p, "user_id": other}):  # Review Focus 3: выигрывает первая доставка
        raw, h = signed(dup)
        r = await api.post("/webhooks/payments", content=raw, headers=h)
        assert r.status_code == 200 and r.json() == {"credited": False}
    assert await balance(api, p["user_id"]) == 100
    assert len(await ledger_rows(db, p["user_id"])) == 1
    assert await ledger_rows(db, other) == []
    # отклонённый дубль откатывается целиком и не оставляет пустого пользователя
    assert (await api.get(f"/users/{other}/balance")).status_code == 404


async def test_non_succeeded_payment_does_not_credit(api, db):
    p = payment(fresh_id(), status="failed")
    raw, h = signed(p)
    r = await api.post("/webhooks/payments", content=raw, headers=h)
    assert r.status_code == 200 and r.json() == {"credited": False}
    assert await ledger_rows(db, p["user_id"]) == []


async def test_invalid_or_missing_signature_401(api):
    p = payment(fresh_id())
    raw, h = signed(p)
    bad = await api.post("/webhooks/payments", content=raw, headers={**h, "X-Signature": "0" * 64})
    missing = await api.post("/webhooks/payments", content=raw, headers={"Content-Type": "application/json"})
    non_ascii = await api.post("/webhooks/payments", content=raw, headers={**h, "X-Signature": b"\xe9\xe9"})
    assert bad.status_code == missing.status_code == non_ascii.status_code == 401
    assert bad.json() == {"detail": "invalid signature"}
    assert (await api.get(f"/users/{p['user_id']}/balance")).status_code == 404


async def test_signature_is_over_raw_body(api):
    uid, pid = fresh_id(), uuid.uuid4().hex
    raw = f'{{ "payment_id" : "{pid}",  "user_id" : {uid}, "tokens" : 100 , "status" : "succeeded" }}'.encode()
    assert raw != json.dumps(json.loads(raw)).encode()  # пересериализация дала бы другие байты
    h = {"X-Signature": sign(raw, WEBHOOK_SECRET), "Content-Type": "application/json"}
    r = await api.post("/webhooks/payments", content=raw, headers=h)
    assert r.status_code == 200 and r.json() == {"credited": True}
    assert await balance(api, uid) == 100


async def test_balance_endpoint_shows_ledger(api):
    p = payment(fresh_id())
    raw, h = signed(p)
    await api.post("/webhooks/payments", content=raw, headers=h)
    body = (await api.get(f"/users/{p['user_id']}/balance")).json()
    assert body["user_id"] == p["user_id"] and body["balance"] == 100
    assert {k: body["ledger"][0][k] for k in ("amount", "reason", "payment_id")} == {
        "amount": 100, "reason": "payment", "payment_id": p["payment_id"],
    }
    assert (await api.get(f"/users/{fresh_id()}/balance")).status_code == 404


async def test_tokens_must_be_positive(api, db):
    p = payment(fresh_id(), tokens=0)
    raw, h = signed(p)
    assert (await api.post("/webhooks/payments", content=raw, headers=h)).status_code == 422
    assert await ledger_rows(db, p["user_id"]) == []


async def test_out_of_bigint_range_is_422_not_500(api):
    too_big = 2**63
    for p in (payment(too_big), payment(fresh_id(), tokens=too_big)):
        raw, h = signed(p)
        assert (await api.post("/webhooks/payments", content=raw, headers=h)).status_code == 422
    assert (await api.get(f"/users/{too_big}/balance")).status_code == 422
    raw = b"not json"
    h = {"X-Signature": sign(raw, WEBHOOK_SECRET), "Content-Type": "application/json"}
    assert (await api.post("/webhooks/payments", content=raw, headers=h)).status_code == 422


def test_sign_verify():
    s = sign(b"body", "k")
    assert verify(b"body", s, "k")
    assert not verify(b"body ", s, "k") and not verify(b"body", s, "other") and not verify(b"body", None, "k")
    assert verify(b"x", "é", "k") is False  # не-ASCII заголовок -> False, а не TypeError (500)


async def test_refund_task_refunds_charge_once(api, db):
    uid = await new_user(api, 10)
    task_id = await db.fetchval(
        # failed: состояние сразу после перехода running -> failed, внутри которого и вызывается refund_task
        "INSERT INTO tasks (user_id, model_slug, prompt, status, finished_at) "
        "VALUES ($1, 'image-fast', 'p', 'failed', now()) RETURNING id", uid
    )
    await db.execute("INSERT INTO ledger (user_id, amount, reason, task_id) VALUES ($1, -2, 'charge', $2)", uid, task_id)
    async with db.transaction():
        assert await refund_task(db, task_id) is True
        assert await refund_task(db, task_id) is False
    assert [r["amount"] for r in await ledger_rows(db, uid, "refund")] == [2]
    assert await balance(api, uid) == 10
