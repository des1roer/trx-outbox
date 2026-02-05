FROM python:3.12

RUN apt-get update
RUN pip install psycopg2-binary==2.9.7 confluent-kafka==2.3.0

WORKDIR /app
COPY . .

CMD ["python", "outbox_processor.py"]
