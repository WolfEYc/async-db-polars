from abc import ABC, abstractmethod
from typing import Optional

import polars as pl
from polars.type_aliases import SchemaDict


class DB(ABC):
    @abstractmethod
    async def fetch(
        self,
        query: str,
        *,
        schema_overrides: Optional[SchemaDict] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Optional[pl.DataFrame]:
        """
        Used to fetch data from the database into a dataframe.
        Keyword arguments are used for query arguments using :query_param syntax.

        Parameters
        ----------
        query : str
            SQL query to execute
        timeout : Optional[float]
            timeout to give up in seconds. Defaults to None.

        Returns
        ----------
        (Dataframe | None): returns a dataframe if there are results, otherwise None

        Examples
        ----------
        >>> await db.fetch("SELECT * FROM item WHERE id = :id", id=83473)
        shape: (1, 3)
        ┌────────┬─────────┬──────────────┐
        │ id     ┆ name    ┆ price        │
        │ ---    ┆ ---     ┆ ---          │
        │ i64    ┆ str     ┆ decimal[*,2] │
        ╞════════╪═════════╪══════════════╡
        │ 83473  ┆ pumpkin ┆ 12.99        │
        └────────┴─────────┴──────────────┘
        >>> await db.fetch(
                "SELECT * FROM item WHERE id = ANY(:ids) ORDER BY price LIMIT 10",
                ids=[83473, 348374],
            )
        shape: (2, 3)
        ┌────────┬─────────┬──────────────┐
        │ id     ┆ name    ┆ price        │
        │ ---    ┆ ---     ┆ ---          │
        │ i64    ┆ str     ┆ decimal[*,2] │
        ╞════════╪═════════╪══════════════╡
        │ 348374 ┆ yogurt  ┆ 3.99         │
        │ 83473  ┆ pumpkin ┆ 12.99        │
        └────────┴─────────┴──────────────┘

        you can query using series as well + using a dict to pass in the arguments

        >>> ids = pl.Series("id", [83473, 348374])
        >>> names = pl.Series("name", ["onion", "bbq", "beans"])
        >>> attr_dict = {"ids": ids, "names": names}
        >>> query = \"\"\"--sql
            SELECT *
            FROM item
            WHERE
                id = ANY(:ids) OR
                name = ANY(:names)
            ORDER BY price
            LIMIT 10
            \"\"\"

        >>> await db.fetch(
                query,
                **attr_dict,
            )
        shape: (5, 3)
        ┌─────────┬─────────┬──────────────┐
        │ id      ┆ name    ┆ price        │
        │ ---     ┆ ---     ┆ ---          │
        │ i64     ┆ str     ┆ decimal[*,2] │
        ╞═════════╪═════════╪══════════════╡
        │ 1965488 ┆ onion   ┆ 2.99         │
        │ 348374  ┆ yogurt  ┆ 3.99         │
        │ 1870644 ┆ bbq     ┆ 6.99         │
        │ 83473   ┆ pumpkin ┆ 12.99        │
        │ 6334    ┆ beans   ┆ 69.99        │
        └─────────┴─────────┴──────────────┘

        How to Delete

        >>> await db.fetch(
                "DELETE FROM item WHERE id = ANY(:ids) RETURNING id",
                ids=[394789, 3483, 39]
            )
        shape: (3, 1)
        ┌────────┐
        │ id     │
        │ ---    │
        │ i64    │
        ╞════════╡
        │ 3483   │
        │ 39     │
        │ 394789 │
        └────────┘
        """
        ...

    @abstractmethod
    async def insert(
        self,
        df: pl.DataFrame,
        table_name: str,
        *,
        pkey_cols: Optional[set[str]] = None,
        return_cols: Optional[set[str]] = None,
        return_schema_overrides: Optional[SchemaDict] = None,
        timeout: Optional[float] = None,
    ) -> Optional[pl.DataFrame]:
        """
        Used to insert a dataframe into a table, optionally upserting on pkey_cols

        Parameters
        ----------
            df : pl.DataFrame
                dataframe to insert
            table_name : str
                table to insert into
            pkey_cols : Optional[set[str]]
                if arbitrary pkeys are specified, will be used to attempt upsert operation. Defaults to None.
            timeout : Optional[float]
                timeout for db connection. Defaults to None.
        Returns
        ----------
            (pl.DataFrame | None): returns a dataframe if there are results, otherwise None

        Examples
        ----------
        >>> await db.fetch("SELECT * FROM item")
        shape: (11, 3)
        ┌──────────┬───────────┬──────────────┐
        │ id       ┆ name      ┆ price        │
        │ ---      ┆ ---       ┆ ---          │
        │ i64      ┆ str       ┆ decimal[*,2] │
        ╞══════════╪═══════════╪══════════════╡
        │ 394789   ┆ beanie    ┆ 16.89        │
        │ 1965488  ┆ onion     ┆ 2.99         │
        │ 1870644  ┆ bbq       ┆ 6.99         │
        │ 69420    ┆ condoms   ┆ 69.42        │
        │ …        ┆ …         ┆ …            │
        │ 22334    ┆ poop      ┆ 100.99       │
        │ 348374   ┆ yogurt    ┆ 3.99         │
        │ 98333829 ┆ chocolate ┆ 4.99         │
        │ 83473    ┆ pumpkin   ┆ 12.99        │
        └──────────┴───────────┴──────────────┘
        >>> items_to_insert = pl.DataFrame(
                {
                    "id": [394789, 3483, 39],
                    "name": ["beanie", "foo", "bar"],
                    "price": [12.66, 10.0, 20.0],
                }
            )

        Note: beanie already exists, but the price is different.

        >>> await db.insert(items_to_insert, "item", return_cols={"id"})
        shape: (2, 1)
        ┌──────┐
        │ id   │
        │ ---  │
        │ i64  │
        ╞══════╡
        │ 3483 │
        │ 39   │
        └──────┘

        Note: beanie was not inserted, since it already exists, which is the default action.

        I am now going to add id to the pkey_cols,
        so that we can upsert on the constraint column
        >>> await db.insert(
                items_to_insert,
                "item",
                pkey_cols={"id"},
                return_cols={"id"}
            )
        shape: (1, 1)
        ┌────────┐
        │ id     │
        │ ---    │
        │ i64    │
        ╞════════╡
        │ 394789 │
        └────────┘

        Note: The price of beanie is now updated.\n
        beanie is the only row returned since it is the only row that was updated.
        """
        ...
