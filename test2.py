from __future__ import annotations

import os

import pymysql
from dotenv import load_dotenv

from schema import ensure_tables


def main() -> None:
    load_dotenv("/Users/hsh/ktbot/.env")

    conn = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "13306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "ktbot"),
        charset=os.getenv("DB_CHARSET", "utf8mb4"),
        autocommit=False,
    )

    try:
        ensure_tables(conn)
        print("schema applied")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
