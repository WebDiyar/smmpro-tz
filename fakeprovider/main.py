"""Д7: фейковый провайдер. Одно приложение, два контейнера (alpha/beta) - отличаются только PROVIDER_NAME.

Один воркер намеренно: режим хранится в памяти процесса, для тестового двойника это допустимо.
"""
import asyncio
import json
import logging
import os
import random
import uuid
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from app.signing import sign

log = logging.getLogger("fakeprovider")

NAME = os.environ.get("PROVIDER_NAME", "fake")
SECRET = os.environ["WEBHOOK_SECRET"]


class FakeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail_rate: float = 0
    fail_type: Literal["technical", "moderation", "payload"] = "technical"
    submit_error: Literal["none", "503", "timeout"] = "none"
    submit_sleep: float = 30  # сон перед ответом в режиме timeout
    webhook: Literal["once", "twice_sequential", "twice_concurrent", "never"] = "once"
    delay_min: float = 2  # поздний completed = delay_min > PROVIDER_TIMEOUT
    delay_max: float = 20


def defaults() -> FakeConfig:
    return FakeConfig(**json.loads(os.environ.get("FAKE_CONFIG") or "{}"))


config: FakeConfig = defaults()
deliveries: set[asyncio.Task] = set()
client = httpx.AsyncClient(timeout=10)  # тесты подменяют на клиент с MockTransport

app = FastAPI(title=f"fake provider {NAME}")


class TaskIn(BaseModel):
    reference: str
    prompt: str
    ref_images: list[str] = []
    callback_url: str


async def post_webhook(url: str, body: bytes) -> None:
    # Фейк не повторяет доставку: ошибку логируем и забываем.
    try:
        await client.post(url, content=body, headers={"X-Signature": sign(body, SECRET), "Content-Type": "application/json"})
    except Exception:
        log.warning("webhook to %s failed", url, exc_info=True)


async def deliver(task: TaskIn, external_id: str, cfg: FakeConfig) -> None:
    await asyncio.sleep(random.uniform(cfg.delay_min, cfg.delay_max))
    payload = {"reference": task.reference, "external_id": external_id}
    if random.random() < cfg.fail_rate:
        payload |= {"status": "failed", "error_type": cfg.fail_type}
    else:
        payload |= {"status": "completed", "result_url": f"https://files.example/{external_id}.png"}
    body = json.dumps(payload).encode()  # одни и те же байты для обеих доставок
    if cfg.webhook == "twice_concurrent":
        await asyncio.gather(post_webhook(task.callback_url, body), post_webhook(task.callback_url, body))
        return
    await post_webhook(task.callback_url, body)
    if cfg.webhook == "twice_sequential":
        await post_webhook(task.callback_url, body)


@app.post("/tasks", status_code=202)
async def submit(task: TaskIn):
    cfg = config  # снимок: смена режима не влияет на уже принятую задачу
    if cfg.submit_error == "503":
        raise HTTPException(503, "provider unavailable")
    external_id = uuid.uuid4().hex
    if cfg.webhook != "never":
        t = asyncio.create_task(deliver(task, external_id, cfg))
        deliveries.add(t)
        t.add_done_callback(deliveries.discard)
    if cfg.submit_error == "timeout":
        # Задача принята (вебхук уже запланирован), но ответ не приходит: клиент отваливается по тайм-ауту.
        await asyncio.sleep(cfg.submit_sleep)
    return {"external_id": external_id}


@app.put("/_admin/config")
async def put_config(patch: dict):
    global config
    try:
        config = FakeConfig(**{**config.model_dump(), **patch})
    except ValidationError as e:
        raise HTTPException(422, json.loads(e.json()))
    return config


@app.post("/_admin/reset")
async def reset():
    """Дефолтный конфиг и отмена ещё не доставленных вебхуков, чтобы тесты не влияли друг на друга."""
    global config
    config = defaults()
    for t in list(deliveries):
        t.cancel()
    deliveries.clear()
    return config
