import os

import asyncpg
from dotenv import load_dotenv

from pg.pgdb import PGDB

db = PGDB()

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


async def init():
    pool = await asyncpg.create_pool(DATABASE_URL)
    if pool is None:
        raise Exception("Could not connect to database")
    db.init(pool)


async def close():
    await db.pool.close()
