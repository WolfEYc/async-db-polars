from functools import reduce
from typing import Awaitable, Callable, Optional

import asyncpg
import polars as pl
from polars.type_aliases import SchemaDict

from async_db_polars.db import DB


def res_to_df(
    res,
    schema_overrides: Optional[SchemaDict] = None,
):
    if not res or len(res) == 0:
        return None
    res_values = map(lambda r: list(r.values()), res)
    res_columns = list(res[0].keys())
    df = pl.DataFrame(res_values, res_columns, schema_overrides=schema_overrides)
    return df


def get_non_pkey_col_sql(df: pl.DataFrame, pkey_cols: set[str], table_name: str):
    non_key_cols = set(df.columns).difference(pkey_cols)
    non_key_cols_sqls = map(lambda x: f"{x} = EXCLUDED.{x}", non_key_cols)
    non_key_cols_sql = ", ".join(non_key_cols_sqls)
    where_conditions_sqls = map(
        lambda x: f"{table_name}.{x} != EXCLUDED.{x}", non_key_cols
    )
    where_condition_sql = " OR ".join(where_conditions_sqls)
    upsert_sql = f"{non_key_cols_sql} WHERE {where_condition_sql}"
    return upsert_sql


def kwargs_to_sql(query: str, **kwargs):
    return reduce(
        lambda acc, x: acc.replace(f":{x[1]}", f"${x[0]}"),
        enumerate(kwargs.keys(), 1),
        query,
    )


POLARS_TO_POSTGRES_TYPE_MAP = {
    str(pl.Object): "UUID",
    str(pl.Boolean): "BOOLEAN",
    str(pl.UInt8): "SMALLINT",
    str(pl.UInt16): "SMALLINT",
    str(pl.UInt32): "INT",
    str(pl.UInt64): "BIGINT",
    str(pl.Int8): "SMALLINT",
    str(pl.Int16): "SMALLINT",
    str(pl.Int32): "INT",
    str(pl.Int64): "BIGINT",
    str(pl.Float32): "REAL",
    str(pl.Float64): "DOUBLE PRECISION",
    "Datetime": "TIMESTAMP",
    "Date": "DATE",
    str(pl.Utf8): "TEXT",
    str(pl.String): "TEXT",
}


def map_to_postgres_type(polars_type: str):
    try:
        return next(
            filter(lambda x: x[0] in polars_type, POLARS_TO_POSTGRES_TYPE_MAP.items())
        )[1]
    except StopIteration:
        raise ValueError(f"Polars type {polars_type} is not supported by Postgres.")


class PGDB(DB):
    pool: Optional[asyncpg.Pool] = None
    init_func: Optional[Callable[[], Awaitable[asyncpg.Pool]]] = None

    def __init__(
        self,
        pool_or_pool_factory: asyncpg.Pool | Callable[[], Awaitable[asyncpg.Pool]],
    ):
        if isinstance(pool_or_pool_factory, asyncpg.Pool):
            self.pool = pool_or_pool_factory
        else:
            self.init_func = pool_or_pool_factory

    async def close(self):
        if self.pool is not None:
            await self.pool.close()

    async def fetch(
        self,
        query: str,
        *,
        schema_overrides: Optional[SchemaDict] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ):
        if self.pool is None:
            self.pool = await self.init_func()  # type: ignore

        numbered_args_query = kwargs_to_sql(query, **kwargs)
        res = await self.pool.fetch(
            numbered_args_query, *kwargs.values(), timeout=timeout
        )
        return res_to_df(res, schema_overrides)

    async def insert(
        self,
        df: pl.DataFrame,
        table_name: str,
        *,
        pkey_cols: Optional[set[str]] = None,
        return_cols: Optional[set[str]] = None,
        return_schema_overrides: Optional[SchemaDict] = None,
        timeout: Optional[float] = None,
    ):
        if self.pool is None:
            self.pool = await self.init_func()  # type: ignore

        cols_sql = ", ".join(df.columns)

        placeholder_fillers = ", ".join(
            map(
                lambda x: f"${x[0]}::{map_to_postgres_type(str(x[1][1]))}[]",
                enumerate(df.schema.items(), 1),
            )
        )

        conflict_resolution = (
            f"ON CONFLICT ({', '.join(pkey_cols)}) DO UPDATE SET {get_non_pkey_col_sql(df, pkey_cols, table_name)}"
            if pkey_cols
            else "ON CONFLICT DO NOTHING"
        )

        return_cols_sql = f"RETURNING {", ".join(return_cols)}" if return_cols else ""

        query = f"INSERT INTO {table_name} ({cols_sql}) SELECT * FROM UNNEST({placeholder_fillers}) {conflict_resolution} {return_cols_sql}"
        data = df.get_columns()

        res = await self.pool.fetch(
            query,
            *data,
            timeout=timeout,  # type: ignore
        )

        return res_to_df(res, return_schema_overrides)
