"""Движения денег. Баланс меняет только триггер на вставке в ledger (Д1), здесь - только вставки."""
from uuid import UUID


class _Duplicate(Exception):
    pass


async def credit_payment(pool, payment_id: str, user_id: int, tokens: int) -> bool:
    """Начисление ровно один раз: повторные и одновременные доставки ждут на UNIQUE(payment_id) и становятся no-op.

    Дубль откатывает транзакцию целиком: иначе повтор payment_id с чужим user_id оставил бы пустого пользователя.
    """
    try:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)
            row = await conn.fetchrow(
                "INSERT INTO ledger (user_id, amount, reason, payment_id) VALUES ($1, $2, 'payment', $3) "
                "ON CONFLICT (payment_id) DO NOTHING RETURNING id",
                user_id, tokens, payment_id,
            )
            if row is None:
                raise _Duplicate
    except _Duplicate:
        return False
    return True


async def refund_task(conn, task_id: UUID) -> bool:
    """Возврат суммы списания. Вызывать только внутри транзакции перехода running -> failed (Д1)."""
    row = await conn.fetchrow(
        "INSERT INTO ledger (user_id, amount, reason, task_id) "
        "SELECT user_id, -amount, 'refund', task_id FROM ledger WHERE task_id = $1 AND reason = 'charge' "
        "ON CONFLICT (task_id, reason) DO NOTHING RETURNING id",
        task_id,
    )
    return bool(row)


async def get_balance(pool, user_id: int, limit: int = 20) -> dict | None:
    # repeatable_read: баланс и журнал из одного снимка
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        balance = await conn.fetchval("SELECT balance FROM users WHERE id = $1", user_id)
        if balance is None:
            return None
        rows = await conn.fetch(
            "SELECT id, amount, reason, payment_id, task_id, created_at FROM ledger "
            "WHERE user_id = $1 ORDER BY id DESC LIMIT $2",
            user_id, limit,
        )
    return {"user_id": user_id, "balance": balance, "ledger": [dict(r) for r in rows]}
