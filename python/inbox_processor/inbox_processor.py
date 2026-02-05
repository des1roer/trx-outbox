import os
import json
import time
import decimal
import random
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError

DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
FAULTY = float(os.getenv("FAULTY", 0))


def random_fail():
    if FAULTY:
        if random.random() < FAULTY:
            raise Exception("Something went wrong... Status unknown.")


G = "\033[1;32m"
R = "\033[1;31m"
C = "\033[0m"


def init_db(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS inbox (
        transaction_id BIGINT PRIMARY KEY,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS balance (
        account_id INT PRIMARY KEY,
        current_balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    conn.commit()
    cursor.close()


def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            print(f"{R}Failed to connect to database: {e}{C}")
            time.sleep(1)


def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'inbox_processor',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'group.instance.id': 'the_only_client',
    })

    notice = False

    while True:
        try:
            meta = consumer.list_topics(topic=KAFKA_TOPIC, timeout=5).topics[KAFKA_TOPIC]
            if not meta.partitions or meta.error and meta.error.code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                if not notice:
                    print("Waiting for topic to be available...")
                    notice = True
                time.sleep(1)
            else:
                consumer.subscribe([KAFKA_TOPIC])
                break
        except Exception as e:
            print(f"{R}Error getting meta for topic: {e}{C}")
            time.sleep(1)

    conn = get_db_connection()
    init_db(conn)
    cursor = conn.cursor()

    notice = False
    msg = None

    while True:
        if msg is None:
            msg = consumer.poll(1.0)

        if msg is None:
            if not notice:
                print("Wait for messages")
            notice = True
            continue

        if msg.error():
            print(f"{R}Consumer error: {msg.error()} at offset {msg.offset()}{C}")

            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"{G}End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}{C}")
                continue
            elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                if not notice:
                    print("Waiting for topic to appear...")
                    notice = True
                time.sleep(1)
                continue
            elif msg.error():
                print(f"{R}Consumer error: {msg.error()} at offset {msg.offset()}{C}")
                raise KafkaException(msg.error())

        notice = False

        if not conn:
            conn = get_db_connection()
            cursor = conn.cursor()

        transaction_id = None
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            transaction_id = payload['transaction_id']
            print(f"Processing {transaction_id} at {msg.offset()}...")
            account_id = payload['account_id']
            amount = decimal.Decimal(payload['amount'])
            transaction_type = payload['transaction_type']

            # Possible failure: loss connection to DB after consuming
            random_fail()

            cursor.execute('SELECT 1 FROM inbox WHERE transaction_id = %s;', (transaction_id,))
            if cursor.fetchone():
                # Message has already been processed earlier (idempotence checked by inbox).
                # But offset was not confirmed by some reason
                # Commit in Kafka and move on.
                print(
                    f"{G}Transaction {transaction_id} already processed (confirmed by inbox). Commit offset {msg.offset()}{C}")
                consumer.commit(message=msg)
                msg = None
                continue

            if transaction_type == "WITHDRAWAL":
                amount = -amount

            cursor.execute('''
                INSERT INTO balance (account_id, current_balance, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (account_id) DO UPDATE SET current_balance = balance.current_balance + EXCLUDED.current_balance, updated_at = NOW();
            ''', (account_id, amount))

            # Something may happen within transaction. DB txn will handle all or nothing for both inserts.
            random_fail()

            cursor.execute('INSERT INTO inbox (transaction_id) VALUES (%s);', (transaction_id,))

            conn.commit()
            print(
                f"Processed transaction {transaction_id} for account {account_id}. Commiting offset {msg.offset()}...")

            # We may loose connection to DB earlier than receive reply to transaction commit.
            # So in general we don't know if transaction was commited or not.
            # Classic Two Generals Problem
            random_fail()

            consumer.commit(message=msg)
            print(
                f"{G}Processed transaction {transaction_id} for account {account_id}. Commited offset {msg.offset()}{C}")
            msg = None

        except Exception as e:
            print(f"{R}Failed to process message {transaction_id}: {e} at offset {msg.offset()}{C}")
            conn.rollback()
            cursor.close()
            conn.close()
            conn = None


if __name__ == "__main__":
    main()
