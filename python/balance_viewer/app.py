from fastapi import FastAPI, HTTPException
import psycopg2
import os
import logging
import time

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

class EndpointFilter(logging.Filter):
    def __init__(self,path,*args,**kwargs):
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

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS balance (
        account_id INT PRIMARY KEY,
        current_balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    conn.commit()
    cursor.close()
    conn.close()

@app.on_event("startup")
def startup_event():
    init_db()

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
