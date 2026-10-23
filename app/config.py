import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    database_url: str
    webhook_secret: str
    provider_timeout: float = 60
    provider_http_timeout: float = 5
    watchdog_interval: float = 1
    provider_urls: dict[str, str] = field(default_factory=dict)  # {"alpha": url, "beta": url}
    public_url: str = "http://api:8000"

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        return cls(
            database_url=e("DATABASE_URL", "postgresql://app:app@db:5432/app"),
            webhook_secret=os.environ["WEBHOOK_SECRET"],
            provider_timeout=float(e("PROVIDER_TIMEOUT", "60")),
            provider_http_timeout=float(e("PROVIDER_HTTP_TIMEOUT", "5")),
            watchdog_interval=float(e("WATCHDOG_INTERVAL", "1")),
            provider_urls={"alpha": e("ALPHA_URL", "http://alpha:8000"), "beta": e("BETA_URL", "http://beta:8000")},
            public_url=e("PUBLIC_URL", "http://api:8000"),
        )


settings = Settings.from_env()
