"""Фейковый провайдер in-process: ASGITransport на вход, MockTransport на выход (запись вебхуков)."""
import asyncio
import json
import os
import socket

import httpx
import pytest
import pytest_asyncio
import uvicorn

import fakeprovider.main as fp
from app.signing import verify

SECRET = os.environ["WEBHOOK_SECRET"]
TASK = {"reference": "ref-1", "prompt": "cat", "ref_images": [], "callback_url": "http://api.test/webhooks/providers/alpha"}


class Receiver:
    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.in_flight = 0
        self.peak = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        await asyncio.sleep(0.05)  # держим запрос "в полёте", чтобы одновременность была видна
        self.in_flight -= 1
        self.requests.append(request)
        return httpx.Response(200, json={"ok": True})


@pytest_asyncio.fixture
async def rx():
    r = Receiver()
    real = fp.client
    fp.client = httpx.AsyncClient(transport=httpx.MockTransport(r))
    await fp.reset()
    yield r
    await fp.reset()
    await fp.client.aclose()
    fp.client = real


@pytest_asyncio.fixture
async def fake(rx):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=fp.app), base_url="http://fake") as c:
        yield c


async def setup(fake, **cfg):
    r = await fake.put("/_admin/config", json={"delay_min": 0, "delay_max": 0, **cfg})
    assert r.status_code == 200, r.text


async def submit(fake, **over):
    return await fake.post("/tasks", json={**TASK, **over})


async def drain():
    await asyncio.gather(*fp.deliveries)


async def test_accepts_task_202_with_external_id(fake, rx):
    await setup(fake)
    r = await submit(fake)
    assert r.status_code == 202 and r.json()["external_id"]
    await drain()


async def test_completed_webhook_is_signed(fake, rx):
    await setup(fake)
    ext = (await submit(fake)).json()["external_id"]
    await drain()
    assert len(rx.requests) == 1
    req = rx.requests[0]
    assert str(req.url) == TASK["callback_url"]
    assert verify(req.content, req.headers["X-Signature"], SECRET)
    body = json.loads(req.content)
    assert body["status"] == "completed" and body["reference"] == "ref-1" and body["external_id"] == ext
    assert body["result_url"]


async def test_fail_rate_one_sends_failed_with_type(fake, rx):
    await setup(fake, fail_rate=1, fail_type="moderation")
    await submit(fake)
    await drain()
    body = json.loads(rx.requests[0].content)
    assert body["status"] == "failed" and body["error_type"] == "moderation" and "result_url" not in body


async def test_submit_503(fake, rx):
    await setup(fake, submit_error="503")
    assert (await submit(fake)).status_code == 503
    await drain()
    assert rx.requests == []


@pytest_asyncio.fixture
async def live_url():
    """ASGITransport не знает про тайм-ауты, поэтому для ReadTimeout нужен настоящий сокет."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(fp.app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    await task


async def test_submit_timeout_sleeps(live_url, rx):
    async with httpx.AsyncClient(base_url=live_url, timeout=httpx.Timeout(5.0, read=0.3)) as c:
        await c.put("/_admin/config", json={"submit_error": "timeout", "submit_sleep": 1, "webhook": "never"})
        with pytest.raises(httpx.ReadTimeout):
            await c.post("/tasks", json=TASK)


async def test_twice_concurrent_sends_two_identical(fake, rx):
    await setup(fake, webhook="twice_concurrent")
    await submit(fake)
    await drain()
    assert len(rx.requests) == 2
    assert rx.requests[0].content == rx.requests[1].content
    assert all(verify(r.content, r.headers["X-Signature"], SECRET) for r in rx.requests)
    assert rx.peak == 2  # реально одновременно


async def test_twice_sequential_sends_two_identical_one_after_another(fake, rx):
    await setup(fake, webhook="twice_sequential")
    await submit(fake)
    await drain()
    assert len(rx.requests) == 2 and rx.requests[0].content == rx.requests[1].content
    assert rx.peak == 1


async def test_never_sends_nothing(fake, rx):
    await setup(fake, webhook="never")
    assert (await submit(fake)).status_code == 202
    await drain()
    await asyncio.sleep(0.1)
    assert rx.requests == []


async def test_late_delay_holds_webhook_until_after_delay(fake, rx):
    await setup(fake, delay_min=0.2, delay_max=0.2)
    await submit(fake)
    await asyncio.sleep(0.1)
    assert rx.requests == []
    await drain()
    assert len(rx.requests) == 1


async def test_reset_restores_defaults_and_cancels_pending(fake, rx):
    await setup(fake, delay_min=5, delay_max=5)
    await submit(fake)
    assert (await fake.post("/_admin/reset")).status_code == 200
    assert fp.config == fp.FakeConfig() and not fp.deliveries
    await asyncio.sleep(0.05)
    assert rx.requests == []


async def test_bad_config_422(fake, rx):
    assert (await fake.put("/_admin/config", json={"fail_type": "nope"})).status_code == 422
    assert (await fake.put("/_admin/config", json={"bogus": 1})).status_code == 422
