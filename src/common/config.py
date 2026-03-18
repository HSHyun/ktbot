from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str | None
    password: str | None
    database: str
    charset: str
    autocommit: bool = False

    def as_pymysql_kwargs(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
            "autocommit": self.autocommit,
        }


@dataclass(frozen=True)
class RabbitMQConfig:
    host: str
    port: int
    user: str
    password: str
    queue_item_summary: str


def load_env_file(path: str | Path) -> None:
    load_dotenv(path)


def db_config_from_env() -> DBConfig:
    return DBConfig(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "13306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "ktbot"),
        charset=os.getenv("DB_CHARSET", "utf8mb4"),
        autocommit=False,
    )


def rabbitmq_config_from_env() -> RabbitMQConfig:
    return RabbitMQConfig(
        host=os.getenv("RABBITMQ_HOST", "127.0.0.1"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        user=os.getenv("RABBITMQ_USER", "guest"),
        password=os.getenv("RABBITMQ_PASSWORD", "guest"),
        queue_item_summary=os.getenv(
            "RABBITMQ_QUEUE_ITEM_SUMMARY", "ktbot.item_summary"
        ),
    )


def required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing {name} in environment.")
    return value

