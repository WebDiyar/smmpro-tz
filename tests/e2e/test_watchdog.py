import asyncio
import os
import time
import uuid

from conftest import configure, ledger_rows, new_user, provider_webhook, wait_task
from test_failover import FAST, finished, history, submit
from test_generate import balance, order

from app import watchdog

PROVIDER_TIMEOUT = float(os.environ["PROVIDER_TIMEOUT"])


def pending(t: dict) -> bool:
    return t["attempts"][-1]["status"] == "pending"


async def nudge(db, *attempt_ids) -> None:
    """Дедлайн попытки - сейчас: проверяется не сам тайм-аут, и ждать 60 с незачем."""
    await db.execute("UPDATE attempts SET deadline_at = now() WHERE id = ANY($1::uuid[])", list(attempt_ids))


async def attempt_url(db, attempt_id: str) -> str | None:
    return await db.fetchval("SELECT result_url FROM attempts WHERE id = $1", uuid.UUID(attempt_id))


async def task_ledger(db, task_id: str) -> list:
    return [r["reason"] for r in await db.fetch(
        "SELECT reason FROM ledger WHERE task_id = $1 ORDER BY id", uuid.UUID(task_id))]


def completed(a: dict, url: str) -> dict:
    return {"reference": a["id"], "external_id": a["external_id"], "status": "completed", "result_url": url}


async def advance_to_second_leg(api, db) -> dict:
    """image-fast без референсов, обе ноги молчат; сторож снимает alpha по дедлайну и ставит beta."""
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    task = await wait_task(api, task_id, pending)
    await nudge(db, task["attempts"][0]["id"])
    return await wait_task(api, task_id, lambda t: len(t["attempts"]) == 2 and pending(t), timeout=5)


async def test_late_completed_after_watchdog_gave_up(api, db):
    """Обязательный №4: сторож снял последнюю ногу по честному PROVIDER_TIMEOUT, затем пришёл completed.

    Правило 6 (Д5): результат не отдаём, возврат остаётся, ссылка - только в истории попытки.
    """
    uid = await new_user(api, 10)
    started = time.monotonic()
    task_id = await submit(api, order(uid, refs=5))  # единственная нога - beta, она молчит
    task = await wait_task(api, task_id, finished, timeout=PROVIDER_TIMEOUT + 15)
    assert time.monotonic() - started >= PROVIDER_TIMEOUT - 1  # сдался по дедлайну, а не раньше
    assert task["status"] == "failed"
    assert history(task) == [("beta", "timed_out", "timeout")]
    assert len(await ledger_rows(db, uid, "refund")) == 1
    a = task["attempts"][0]

    url = "https://files.example/late.png"
    r = await provider_webhook(api, "beta", **completed(a, url))
    assert r.status_code == 200, r.text
    task = (await api.get(f"/tasks/{task_id}")).json()
    assert task["status"] == "failed" and task["result_url"] is None
    assert history(task) == [("beta", "late_completed", "timeout")]
    assert await attempt_url(db, a["id"]) == url
    assert len(await ledger_rows(db, uid, "charge")) == 1
    assert len(await ledger_rows(db, uid, "refund")) == 1
    assert await balance(api, uid) == 10


async def test_watchdog_moves_silent_attempt_to_next_leg(api, db):
    task = await advance_to_second_leg(api, db)
    assert task["status"] == "running"
    assert history(task) == [("alpha", "timed_out", "timeout"), ("beta", "pending", None)]
    assert task["current_leg"]["provider"] == "beta"
    assert await task_ledger(db, task["id"]) == ["charge"]


async def test_superseded_attempt_completed_wins_while_running(api, db):
    """Review Focus 5: completed от снятой сторожем alpha, пока задача идёт на beta, - задача завершается им.

    Снятая попытка становится completed, но error_type 'timeout' сохраняет: это след того, что сторож
    её снимал. Возврата нет и уже не будет.
    """
    task = await advance_to_second_leg(api, db)
    alpha, beta = task["attempts"]
    url = "https://files.example/superseded.png"
    r = await provider_webhook(api, "alpha", **completed(alpha, url))
    assert r.status_code == 200, r.text
    task = (await api.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "completed" and task["result_url"] == url
    assert history(task) == [("alpha", "completed", "timeout"), ("beta", "abandoned", None)]
    assert await attempt_url(db, alpha["id"]) == url
    assert await task_ledger(db, task["id"]) == ["charge"]

    r = await provider_webhook(api, "beta", reference=beta["id"], external_id=beta["external_id"],
                               status="failed", error_type="technical")
    assert r.status_code == 200, r.text
    assert (await api.get(f"/tasks/{task['id']}")).json() == task
    assert await task_ledger(db, task["id"]) == ["charge"]


async def test_stale_failed_on_non_last_leg_is_noop(api, db):
    """Повтор failed от уже отказавшей alpha, когда задача идёт на beta: 200 и ничего не меняется (Д5)."""
    await configure("alpha", fail_rate=1, fail_type="technical", **FAST)
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    task = await wait_task(api, task_id, lambda t: len(t["attempts"]) == 2 and pending(t))
    assert history(task) == [("alpha", "failed", "technical"), ("beta", "pending", None)]
    alpha = task["attempts"][0]
    r = await provider_webhook(api, "alpha", reference=alpha["id"], external_id=alpha["external_id"],
                               status="failed", error_type="technical")
    assert r.status_code == 200, r.text
    assert (await api.get(f"/tasks/{task_id}")).json() == task
    assert await task_ledger(db, task_id) == ["charge"]


async def test_completed_races_watchdog_never_both(api, db, pool):
    """completed и сторож одновременно за одну задачу: либо результат, либо возврат, никогда оба (Д4).

    Дедлайны всех 8 задач уже прошли; 8 вебхуков (через 4 воркера) и 4 прямых tick() стартуют разом,
    сторожа 4 воркеров сервиса тоже работают. Распределение исходов не проверяется.
    """
    uids = await asyncio.gather(*[new_user(api, 10) for _ in range(8)])
    task_ids = [await submit(api, order(uid, refs=5)) for uid in uids]  # одна нога - beta, молчит
    tasks = await asyncio.gather(*[wait_task(api, t, pending) for t in task_ids])
    attempts = [t["attempts"][0] for t in tasks]
    await db.execute("UPDATE attempts SET deadline_at = now() - interval '1 s' WHERE id = ANY($1::uuid[])",
                     [a["id"] for a in attempts])

    results = await asyncio.gather(
        *[provider_webhook(api, "beta", **completed(a, f"https://files.example/{a['id']}.png")) for a in attempts],
        *[watchdog.tick(pool) for _ in range(4)],
    )
    assert [r.status_code for r in results[:8]] == [200] * 8

    outcomes = []
    for uid, task_id in zip(uids, task_ids):
        task = (await api.get(f"/tasks/{task_id}")).json()
        refunds = len(await ledger_rows(db, uid, "refund"))
        [a] = task["attempts"]
        if task["status"] == "completed":
            assert refunds == 0 and task["result_url"] and a["status"] == "completed", task
            assert await balance(api, uid) == 8
        else:
            assert task["status"] == "failed" and task["result_url"] is None, task
            assert refunds == 1 and a["status"] == "late_completed", task
            assert await balance(api, uid) == 10
        outcomes.append(task["status"])
    print("race outcomes:", sorted(outcomes))


async def test_orphaned_dispatching_attempt_recovered(api, db):
    """Review Focus 2: процесс упал между COMMIT попытки и HTTP - попытка осталась в dispatching."""
    uid = await new_user(api, 10)
    task_id = await submit(api, order(uid))
    task = await wait_task(api, task_id, pending)
    await db.execute("UPDATE attempts SET status = 'dispatching', deadline_at = now() WHERE id = $1",
                     uuid.UUID(task["attempts"][0]["id"]))
    task = await wait_task(api, task_id, lambda t: len(t["attempts"]) == 2, timeout=5)
    assert task["status"] == "running"
    assert task["attempts"][0]["status"] == "timed_out"
    assert task["attempts"][1]["provider"] == "beta"


async def test_watchdog_does_not_double_advance(api, db):
    """5 просроченных задач одновременно на 4 сторожах: каждая продвинута ровно один раз."""
    uids = await asyncio.gather(*[new_user(api, 10) for _ in range(5)])
    task_ids = [await submit(api, order(uid)) for uid in uids]
    tasks = await asyncio.gather(*[wait_task(api, t, pending) for t in task_ids])
    await nudge(db, *[t["attempts"][0]["id"] for t in tasks])
    await asyncio.sleep(5)
    for task_id in task_ids:
        task = (await api.get(f"/tasks/{task_id}")).json()
        assert history(task) == [("alpha", "timed_out", "timeout"), ("beta", "pending", None)], task
        assert await task_ledger(db, task_id) == ["charge"]


async def test_tick_survives_poisoned_attempt(api, db, pool, monkeypatch):
    """Сбой fail_attempt на самой старой попытке не должен останавливать тик (ORDER BY deadline_at LIMIT 1).

    Сторожа сервиса не мешают: дедлайны в будущем, а OVERDUE в тесте (только в процессе теста) выбирает
    ровно две наши попытки без условия по дедлайну. Отравленная - с более ранним дедлайном, значит первая.
    """
    uids = await asyncio.gather(*[new_user(api, 10) for _ in range(2)])
    task_ids = [await submit(api, order(uid)) for uid in uids]
    poison, good = [(await wait_task(api, t, pending))["attempts"][0]["id"] for t in task_ids]
    for aid, secs in ((poison, 30), (good, 40)):
        await db.execute("UPDATE attempts SET deadline_at = now() + make_interval(secs => $2) WHERE id = $1",
                         uuid.UUID(aid), secs)

    monkeypatch.setattr(watchdog, "OVERDUE", watchdog.OVERDUE.replace(
        "a.deadline_at < now()", f"a.id = ANY(ARRAY['{poison}','{good}']::uuid[])"))
    real = watchdog.orchestrator.fail_attempt

    async def flaky(conn, attempt_id, error_type):
        if str(attempt_id) == poison:
            raise RuntimeError("poisoned")
        return await real(conn, attempt_id, error_type)

    monkeypatch.setattr(watchdog.orchestrator, "fail_attempt", flaky)
    new_ids = await watchdog.tick(pool)

    good_task = (await api.get(f"/tasks/{task_ids[1]}")).json()
    assert history(good_task)[0] == ("alpha", "timed_out", "timeout") and len(good_task["attempts"]) == 2
    assert new_ids == [uuid.UUID(good_task["attempts"][1]["id"])]
    poisoned_task = (await api.get(f"/tasks/{task_ids[0]}")).json()
    assert history(poisoned_task) == [("alpha", "pending", None)]
