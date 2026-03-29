from __future__ import annotations


def _index_exists(cur, table_name: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        LIMIT 1;
        """,
        (table_name, index_name),
    )
    return cur.fetchone() is not None


def _create_index_if_missing(cur, table_name: str, index_name: str, ddl: str) -> None:
    if not _index_exists(cur, table_name, index_name):
        cur.execute(ddl)


def ensure_tables(conn) -> None:
    """MySQL용 테이블/인덱스를 생성하고 기본값을 정비한다."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS source (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(50) NOT NULL UNIQUE,
                name VARCHAR(200) NOT NULL,
                url_pattern TEXT NOT NULL,
                parser VARCHAR(100) NOT NULL,
                fetch_interval_minutes INT DEFAULT 60,
                is_active BOOLEAN NOT NULL DEFAULT FALSE,
                metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                source_id INT NOT NULL,
                external_id VARCHAR(200) NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                author TEXT,
                content TEXT,
                published_at TIMESTAMP NULL,
                first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
                CONSTRAINT fk_item_source
                    FOREIGN KEY (source_id) REFERENCES source(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_asset (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                item_id BIGINT NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                url TEXT,
                local_path TEXT,
                metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_item_asset_item
                    FOREIGN KEY (item_id) REFERENCES item(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        _create_index_if_missing(
            cur,
            "item",
            "uq_item_source_external",
            "CREATE UNIQUE INDEX uq_item_source_external ON item (source_id, external_id);",
        )
        _create_index_if_missing(
            cur,
            "item_asset",
            "idx_item_asset_item_id",
            "CREATE INDEX idx_item_asset_item_id ON item_asset (item_id);",
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_summary (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                item_id BIGINT NOT NULL,
                model_name TEXT NOT NULL,
                summary_text TEXT NOT NULL,
                summary_title TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                meta JSON NOT NULL DEFAULT (JSON_OBJECT()),
                CONSTRAINT fk_item_summary_item
                    FOREIGN KEY (item_id) REFERENCES item(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        _create_index_if_missing(
            cur,
            "item_summary",
            "idx_item_summary_item_id",
            "CREATE INDEX idx_item_summary_item_id ON item_summary (item_id);",
        )
        _create_index_if_missing(
            cur,
            "item_summary",
            "idx_item_summary_created_at",
            "CREATE INDEX idx_item_summary_created_at ON item_summary (created_at);",
        )
        _create_index_if_missing(
            cur,
            "item_summary",
            "uq_item_summary_item_model",
            "CREATE UNIQUE INDEX uq_item_summary_item_model ON item_summary (item_id, model_name(191));",
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `comment` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                item_id BIGINT NOT NULL,
                external_id TEXT NOT NULL,
                author TEXT,
                content TEXT,
                created_at TIMESTAMP NULL,
                is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
                parent_id BIGINT NULL,
                CONSTRAINT fk_comment_item
                    FOREIGN KEY (item_id) REFERENCES item(id) ON DELETE CASCADE,
                CONSTRAINT fk_comment_parent
                    FOREIGN KEY (parent_id) REFERENCES `comment`(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        _create_index_if_missing(
            cur,
            "comment",
            "uq_comment_item_external",
            "CREATE UNIQUE INDEX uq_comment_item_external ON `comment` (item_id, external_id(191));",
        )
        _create_index_if_missing(
            cur,
            "comment",
            "idx_comment_item_id",
            "CREATE INDEX idx_comment_item_id ON `comment` (item_id);",
        )
        _create_index_if_missing(
            cur,
            "comment",
            "idx_comment_parent_id",
            "CREATE INDEX idx_comment_parent_id ON `comment` (parent_id);",
        )
        cur.execute(
            """
            ALTER TABLE source
            MODIFY COLUMN is_active BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crawl_run_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                source TEXT NOT NULL,
                queued_count INT NOT NULL,
                fetched_count INT,
                filtered_count INT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kakao_subscription (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                kakao_user_id VARCHAR(191) NOT NULL,
                hours_window INT NOT NULL DEFAULT 6,
                timezone VARCHAR(100) NOT NULL DEFAULT 'Asia/Seoul',
                send_hour TINYINT NOT NULL DEFAULT 8,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                last_sent_window_end TIMESTAMP NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_kakao_subscription_user_window (kakao_user_id, hours_window)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        _create_index_if_missing(
            cur,
            "kakao_subscription",
            "idx_kakao_subscription_schedule",
            "CREATE INDEX idx_kakao_subscription_schedule ON kakao_subscription (is_active, timezone, send_hour);",
        )
    conn.commit()
