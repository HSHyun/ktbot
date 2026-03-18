from __future__ import annotations

import pymysql

from common.config import DBConfig


def connect_db(config: DBConfig):
    return pymysql.connect(**config.as_pymysql_kwargs())

