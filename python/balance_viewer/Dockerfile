FROM python:3.12

RUN apt-get update
RUN pip install fastapi==0.95.2 uvicorn==0.23.2 psycopg2-binary==2.9.7 --force

WORKDIR /app
COPY . .

EXPOSE 8100
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8100"]
