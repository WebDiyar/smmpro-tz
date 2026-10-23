import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Literal
from uuid import UUID

import asyncpg
import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Path, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, ValidationError, model_validator

from app import money, orchestrator, watchdog
from app.config import settings
from app.signing import verify

log = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=20)
    app.state.http = httpx.AsyncClient()
    sweeper = asyncio.create_task(watchdog.run(app.state.pool, app.state.http, settings.watchdog_interval))
    yield
    sweeper.cancel()
    with suppress(asyncio.CancelledError):
        await sweeper
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def signed_body(request: Request) -> bytes:
    """Д6: подпись проверяется над сырыми байтами до парсинга - пересериализованный JSON дал бы другие байты."""
    raw = await request.body()
    if not verify(raw, request.headers.get("X-Signature"), settings.webhook_secret):
        log.warning("invalid signature on %s", request.url.path)
        raise HTTPException(401, "invalid signature")
    return raw


BIGINT_MAX = 2**63 - 1  # users.id и ledger.amount - bigint; больше -> 422, а не 500 из asyncpg


class PaymentWebhook(BaseModel):
    payment_id: str = Field(min_length=1, max_length=200)
    user_id: int = Field(gt=0, le=BIGINT_MAX)
    tokens: int = Field(gt=0, le=BIGINT_MAX)
    status: str


@app.post("/webhooks/payments")
async def payment_webhook(request: Request, raw: bytes = Depends(signed_body)):
    try:
        p = PaymentWebhook.model_validate_json(raw)
    except ValidationError as e:
        raise RequestValidationError(e.errors(include_url=False))
    if p.status != "succeeded":  # допущение 5: начисляет только succeeded
        return {"credited": False}
    return {"credited": await money.credit_payment(request.app.state.pool, p.payment_id, p.user_id, p.tokens)}


class ProviderWebhook(BaseModel):
    reference: UUID
    external_id: str = Field(min_length=1, max_length=200)
    status: Literal["completed", "failed"]
    result_url: str | None = None
    error_type: Literal["technical", "moderation", "payload"] | None = None

    @model_validator(mode="after")
    def outcome_has_details(self):
        if self.status == "completed" and not self.result_url:
            raise ValueError("completed requires result_url")
        if self.status == "failed" and self.error_type is None:
            raise ValueError("failed requires error_type")
        return self


@app.post("/webhooks/providers/{name}")
async def provider_webhook(name: str, request: Request, background: BackgroundTasks,
                           raw: bytes = Depends(signed_body)):
    if name not in settings.provider_urls:
        raise HTTPException(404, "unknown provider")
    try:
        p = ProviderWebhook.model_validate_json(raw)
    except ValidationError as e:
        raise RequestValidationError(e.errors(include_url=False))
    pool = request.app.state.pool
    try:
        new_id = await orchestrator.apply_provider_webhook(pool, name, p)
    except LookupError:
        raise HTTPException(404, "attempt not found")
    if new_id is not None:  # транзакция уже закоммичена
        background.add_task(orchestrator.dispatch, pool, request.app.state.http, new_id)
    return {"ok": True}


class GenerateRequest(BaseModel):
    user_id: int = Field(gt=0, le=BIGINT_MAX)
    model_slug: str
    prompt: str = Field(min_length=1)
    ref_images: list[str] = []


@app.post("/generate", status_code=202)
async def generate(req: GenerateRequest, request: Request, background: BackgroundTasks):
    pool, http = request.app.state.pool, request.app.state.http
    try:
        task_id, attempt_id = await orchestrator.create_task(pool, req.user_id, req.model_slug, req.prompt, req.ref_images)
    except orchestrator.AdmissionError as e:
        raise HTTPException(e.status_code, e.detail)
    background.add_task(orchestrator.dispatch, pool, http, attempt_id)  # create_task уже закоммитил
    return {"task_id": task_id}


@app.get("/tasks/{task_id}")
async def task_status(request: Request, task_id: UUID):
    body = await orchestrator.get_task(request.app.state.pool, task_id)
    if body is None:
        raise HTTPException(404, "task not found")
    return body


@app.get("/users/{user_id}/balance")
async def user_balance(request: Request, user_id: int = Path(gt=0, le=BIGINT_MAX)):
    body = await money.get_balance(request.app.state.pool, user_id)
    if body is None:
        raise HTTPException(404, "user not found")
    return body
