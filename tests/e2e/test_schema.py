import os
import random
import uuid

import asyncpg
import pytest

from app.migrate import migrate

DATABASE_URL = os.environ["DATABASE_URL"]


def new_user_id() -> int:
    return random.randint(10**9, 2**62)


async def make_user(db, balance: int = 0) -> int:
    uid = new_user_id()
    await db.execute("INSERT INTO users (id) VALUES ($1)", uid)
    if balance:
        await db.execute(
            "INSERT INTO ledger (user_id, amount, reason, payment_id) VALUES ($1, $2, 'payment', $3)",
            uid, balance, uuid.uuid4().hex,
        )
    return uid


async def make_task(db, uid: int) -> uuid.UUID:
    return await db.fetchval(
        "INSERT INTO tasks (user_id, model_slug, prompt) VALUES ($1, 'image-fast', 'p') RETURNING id", uid
    )


async def test_migrate_is_idempotent():
    assert await migrate(DATABASE_URL) == []


async def test_catalog_seeded(db):
    prices = {r["slug"]: r["price"] for r in await db.fetch("SELECT slug, price FROM models")}
    assert prices == {"image-fast": 2, "video-long": 25}
    for slug, expected in {
        "image-fast": [("alpha", 1, 4, False), ("beta", 2, 10, False)],
        "video-long": [("beta", 1, 2, False), ("alpha", 2, 1, True)],
    }.items():
        rows = await db.fetch(
            "SELECT provider, priority, max_ref_images, skip_on_moderation FROM legs "
            "WHERE model_slug = $1 ORDER BY priority", slug)
        assert [tuple(r) for r in rows] == expected


async def test_ledger_insert_moves_balance(db):
    uid = await make_user(db, 10)
    assert await db.fetchval("SELECT balance FROM users WHERE id = $1", uid) == 10


async def test_balance_cannot_go_negative(db):
    uid = await make_user(db, 10)
    task_id = await make_task(db, uid)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO ledger (user_id, amount, reason, task_id) VALUES ($1, -11, 'charge', $2)", uid, task_id)


async def test_ledger_is_append_only(db):
    uid = await make_user(db, 10)
    with pytest.raises(asyncpg.RaiseError):
        await db.execute("UPDATE ledger SET amount = 999 WHERE user_id = $1", uid)
    with pytest.raises(asyncpg.RaiseError):
        await db.execute("DELETE FROM ledger WHERE user_id = $1", uid)


async def test_payment_id_unique(db):
    uid = await make_user(db)
    sql = "INSERT INTO ledger (user_id, amount, reason, payment_id) VALUES ($1, 5, 'payment', $2)"
    pid = uuid.uuid4().hex
    await db.execute(sql, uid, pid)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute(sql, uid, pid)


async def test_one_refund_per_task(db):
    uid = await make_user(db, 10)
    task_id = await make_task(db, uid)
    await db.execute("INSERT INTO ledger (user_id, amount, reason, task_id) VALUES ($1, -2, 'charge', $2)", uid, task_id)
    refund = "INSERT INTO ledger (user_id, amount, reason, task_id) VALUES ($1, 2, 'refund', $2)"
    await db.execute(refund, uid, task_id)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute(refund, uid, task_id)


async def test_one_live_attempt_per_task(db):
    uid = await make_user(db, 10)
    task_id = await make_task(db, uid)
    leg_id = await db.fetchval("SELECT id FROM legs WHERE model_slug='image-fast' AND priority=1")
    sql = (
        "INSERT INTO attempts (task_id, n, leg_id, provider, status, deadline_at) "
        "VALUES ($1, $2, $3, 'alpha', 'pending', now() + interval '60 seconds')"
    )
    await db.execute(sql, task_id, 1, leg_id)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute(sql, task_id, 2, leg_id)
