from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import sqlalchemy as sa
from sqlalchemy import MetaData, Table, func, select, text
from sqlalchemy.engine import Connection, Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database.models import Base  # noqa: E402
from src.database.session import get_database_backend  # noqa: E402

DEFAULT_SOURCE = PROJECT_ROOT / "data" / "database.db"


@dataclass(frozen=True)
class TablePlan:
    name: str
    source_table: Table
    target_table: Table
    columns: tuple[str, ...]


@dataclass(frozen=True)
class TableCopyResult:
    name: str
    source_rows: int
    inserted_rows: int
    columns: tuple[str, ...]


class MigrationError(RuntimeError):
    pass


def normalize_sqlite_url(value: str | Path) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise MigrationError("SQLite 源路径不能为空")
    if raw.startswith("sqlite:///"):
        return raw
    if "://" in raw:
        raise MigrationError("源库必须是 SQLite 文件路径或 sqlite:/// URL")
    return f"sqlite:///{raw}"


def normalize_postgres_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise MigrationError("PostgreSQL 目标库 URL 不能为空")
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://"):]
    if get_database_backend(raw) != "postgresql":
        raise MigrationError("目标库必须是 PostgreSQL 连接字符串")
    if raw.startswith("postgresql://"):
        return "postgresql+psycopg://" + raw[len("postgresql://"):]
    return raw


def chunked_rows(rows: Sequence[dict], chunk_size: int) -> Iterator[list[dict]]:
    if chunk_size <= 0:
        raise MigrationError("chunk_size 必须大于 0")
    for index in range(0, len(rows), chunk_size):
        yield list(rows[index:index + chunk_size])


def reflect_metadata(engine: Engine) -> MetaData:
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata


def build_table_plan(source_metadata: MetaData, target_metadata: MetaData) -> list[TablePlan]:
    ordered_names = [table.name for table in Base.metadata.sorted_tables]
    extra_names = sorted(
        name for name in source_metadata.tables.keys()
        if name in target_metadata.tables and name not in ordered_names
    )
    plan: list[TablePlan] = []
    for table_name in ordered_names + extra_names:
        source_table = source_metadata.tables.get(table_name)
        target_table = target_metadata.tables.get(table_name)
        if source_table is None or target_table is None:
            continue
        column_names = tuple(column.name for column in target_table.columns if column.name in source_table.c)
        if not column_names:
            continue
        plan.append(TablePlan(table_name, source_table, target_table, column_names))
    return plan


def create_source_engine(sqlite_url: str) -> Engine:
    return sa.create_engine(sqlite_url, connect_args={"check_same_thread": False})


def create_target_engine(postgres_url: str) -> Engine:
    return sa.create_engine(postgres_url, pool_pre_ping=True)


def quote_identifier(connection: Connection, value: str) -> str:
    return connection.dialect.identifier_preparer.quote(value)


def truncate_target_tables(connection: Connection, table_names: Iterable[str]) -> None:
    quoted_names = [quote_identifier(connection, name) for name in table_names]
    if not quoted_names:
        return
    connection.execute(text(f"TRUNCATE TABLE {', '.join(quoted_names)} RESTART IDENTITY CASCADE"))


def ensure_target_empty(connection: Connection, plan: Sequence[TablePlan]) -> None:
    non_empty: list[str] = []
    for item in plan:
        count = connection.execute(select(func.count()).select_from(item.target_table)).scalar_one()
        if count:
            non_empty.append(f"{item.name}={count}")
    if non_empty:
        raise MigrationError(
            "目标库不是空库。请加上 --truncate-target 后重试。非空表: " + ", ".join(non_empty[:8])
        )


def fetch_source_rows(connection: Connection, plan: TablePlan) -> list[dict]:
    stmt = select(*(plan.source_table.c[column_name] for column_name in plan.columns))
    result = connection.execute(stmt)
    return [dict(row._mapping) for row in result]


def copy_table(
    source_connection: Connection,
    target_connection: Connection,
    plan: TablePlan,
    batch_size: int,
) -> TableCopyResult:
    rows = fetch_source_rows(source_connection, plan)
    inserted_rows = 0
    if rows:
        insert_stmt = plan.target_table.insert()
        for chunk in chunked_rows(rows, batch_size):
            target_connection.execute(insert_stmt, chunk)
            inserted_rows += len(chunk)
    return TableCopyResult(
        name=plan.name,
        source_rows=len(rows),
        inserted_rows=inserted_rows,
        columns=plan.columns,
    )


def reset_postgres_sequences(connection: Connection, plan: Sequence[TablePlan]) -> None:
    for item in plan:
        primary_key_columns = [column.name for column in item.target_table.primary_key.columns]
        if len(primary_key_columns) != 1:
            continue
        pk_name = primary_key_columns[0]
        pk_column = item.target_table.c.get(pk_name)
        if pk_column is None or not isinstance(pk_column.type, sa.Integer):
            continue
        quoted_table = quote_identifier(connection, item.target_table.name)
        quoted_column = quote_identifier(connection, pk_name)
        sql = text(
            f"SELECT setval(pg_get_serial_sequence('{item.target_table.name}', '{pk_name}'), "
            f"COALESCE(MAX({quoted_column}), 1), MAX({quoted_column}) IS NOT NULL) "
            f"FROM {quoted_table}"
        )
        connection.execute(sql)


def migrate_sqlite_to_postgres(
    source_url: str,
    target_url: str,
    batch_size: int = 500,
    truncate_target: bool = False,
) -> list[TableCopyResult]:
    if batch_size <= 0:
        raise MigrationError("batch_size 必须大于 0")

    normalized_source = normalize_sqlite_url(source_url)
    normalized_target = normalize_postgres_url(target_url)
    if normalized_source == normalized_target:
        raise MigrationError("源库和目标库不能相同")

    source_engine = create_source_engine(normalized_source)
    target_engine = create_target_engine(normalized_target)

    try:
        Base.metadata.create_all(bind=target_engine)
        source_metadata = reflect_metadata(source_engine)
        target_metadata = reflect_metadata(target_engine)
        plan = build_table_plan(source_metadata, target_metadata)
        if not plan:
            raise MigrationError("没有找到可迁移的数据表")

        table_names = [item.name for item in plan]
        results: list[TableCopyResult] = []

        with source_engine.connect() as source_connection:
            with target_engine.begin() as target_connection:
                if truncate_target:
                    truncate_target_tables(target_connection, table_names)
                else:
                    ensure_target_empty(target_connection, plan)

                for item in plan:
                    result = copy_table(source_connection, target_connection, item, batch_size)
                    results.append(result)

                reset_postgres_sequences(target_connection, plan)

        return results
    finally:
        source_engine.dispose()
        target_engine.dispose()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将 SQLite 数据迁移到 PostgreSQL")
    parser.add_argument(
        "--source",
        default=str(DEFAULT_SOURCE),
        help="SQLite 源文件路径或 sqlite:/// URL，默认 data/database.db",
    )
    parser.add_argument(
        "--target",
        default="",
        help="PostgreSQL 目标 URL；为空时读取 APP_DATABASE_URL 或 DATABASE_URL",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="每批插入的行数，默认 500",
    )
    parser.add_argument(
        "--truncate-target",
        action="store_true",
        help="迁移前先清空目标库中的业务表，并重置自增序列",
    )
    return parser


def resolve_target_url(cli_value: str) -> str:
    raw = str(cli_value or "").strip()
    if raw:
        return raw
    return str(os.environ.get("APP_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()


def print_summary(results: Sequence[TableCopyResult]) -> None:
    total_rows = 0
    print("迁移完成，表统计如下：")
    for item in results:
        total_rows += item.inserted_rows
        print(f"- {item.name}: source={item.source_rows} inserted={item.inserted_rows} columns={len(item.columns)}")
    print(f"总计插入 {total_rows} 行")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    target_url = resolve_target_url(args.target)

    try:
        results = migrate_sqlite_to_postgres(
            source_url=args.source,
            target_url=target_url,
            batch_size=args.batch_size,
            truncate_target=args.truncate_target,
        )
    except MigrationError as exc:
        parser.error(str(exc))
        return 2
    except Exception as exc:
        print(f"迁移失败: {exc}", file=sys.stderr)
        return 1

    print_summary(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())