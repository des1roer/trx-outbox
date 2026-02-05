from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os
import time
import json
from snowflake import SnowflakeGenerator
from datetime import datetime
import decimal
import random

import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger(__name__)

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
FAULTY = float(os.getenv("FAULTY", 0))
if FAULTY:
    log.info("Using faulty mode: %s", FAULTY)


def random_fail():
    if FAULTY:
        if random.random() < FAULTY:
            raise Exception("Something went wrong... Status unknown.")


class EndpointFilter(logging.Filter):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = path

    def filter(self, record: logging.LogRecord):
        return record.getMessage().find(self._path) == -1


uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.addFilter(EndpointFilter(path="/balance"))


def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            print("Failed to connect to database: ", e)
            time.sleep(1)


genid = SnowflakeGenerator(1)


class Transaction(BaseModel):
    account_id: int
    amount: float
    transaction_type: str  # 'DEPOSIT' or 'WITHDRAWAL'
    external_id: str


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # drop table transactions; drop table balance; drop table outbox;
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id BIGINT PRIMARY KEY,
        external_id VARCHAR(64) UNIQUE NOT NULL,
        account_id INT NOT NULL,
        amount DECIMAL(10, 2) NOT NULL,
        balance DECIMAL(10, 2) NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS balance (
        account_id INT PRIMARY KEY,
        current_balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS outbox (
        id BIGSERIAL PRIMARY KEY,
        account_id INT NOT NULL,
        transaction_id BIGINT UNIQUE NOT NULL,
        payload JSONB NOT NULL,
        processed BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    conn.commit()
    cursor.close()
    conn.close()


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/")
async def index_page():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT account_id, current_balance FROM balance order by updated_at desc limit 100")
        result = cursor.fetchall()
        if result:
            res = []
            for row in result:
                res.append({"account_id": row[0], "current_balance": row[1]})
            return {
                "accounts": res
            }
        else:
            return {"accounts": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/balance/{account_id}")
async def get_balance(account_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT current_balance FROM balance WHERE account_id = %s;", (account_id,))
        result = cursor.fetchone()
        if result:
            current_balance = result[0]
            return {"account_id": account_id, "current_balance": current_balance}
        else:
            raise HTTPException(status_code=404, detail="Account not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.post("/transactions/")
async def create_transaction(transaction: Transaction):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        decimal.getcontext().prec = 2
        transaction.amount = decimal.Decimal(transaction.amount)

        if transaction.amount <= 0:
            raise HTTPException(status_code=400, detail=f"Wrong amount")

        if transaction.transaction_type == "WITHDRAWAL":
            transaction.amount = -transaction.amount

        # check inbox behavior
        cursor.execute(
            'SELECT id, balance, amount, transaction_type, created_at FROM transactions WHERE external_id = %s;',
            (transaction.external_id,))
        result = cursor.fetchone()
        if result:
            transaction_id, balance, amount, transaction_type, created_at = result
            if amount == transaction.amount and transaction_type == transaction.transaction_type:
                return {
                    "status": "processed",
                    "transaction_id": transaction_id,
                    "balance": str(balance),
                    "created_at": created_at,
                    "duplicate": True,
                }
            else:
                raise HTTPException(status_code=400, detail=f"Duplicate transaction id with different parameters")

        transaction_id = next(genid)

        cursor.execute('''
            INSERT INTO balance (account_id, current_balance, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (account_id) DO
                UPDATE SET current_balance = balance.current_balance + EXCLUDED.current_balance, updated_at = NOW()
                RETURNING current_balance
        ''', (transaction.account_id, transaction.amount))

        balance, = cursor.fetchone()

        if balance < 0:
            raise HTTPException(status_code=400, detail=f"Insufficient balance")

        cursor.execute('''
            INSERT INTO transactions (id, external_id, account_id, amount, balance, transaction_type, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING created_at
        ''', (transaction_id, transaction.external_id, transaction.account_id, transaction.amount, balance,
              transaction.transaction_type))

        created_at, = cursor.fetchone()

        payload = {
            'transaction_id': transaction_id,
            'external_id': transaction.external_id,
            'account_id': transaction.account_id,
            'amount': "{:0.2f}".format(transaction.amount),
            'balance': str(balance),
            'transaction_type': transaction.transaction_type,
            'timestamp': str(created_at),
        }

        # not really interesting: handled by DB transaction.
        # random_fail()

        cursor.execute('''
            INSERT INTO outbox (account_id, transaction_id, payload) VALUES (%s, %s, %s::jsonb)
        ''', (transaction.account_id, transaction_id, json.dumps(payload)))

        conn.commit()

        # Simulate network issue
        random_fail()

        return {
            "status": "processed",
            "transaction_id": transaction_id,
            "balance": str(balance),
            "created_at": str(created_at),
        }

    except HTTPException as e:
        conn.rollback()
        raise e

    except Exception as e:
        conn.rollback()
        log.error("Request failed: %s", repr(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing transaction: {str(e)}")

    finally:
        cursor.close()
        conn.close()
