run:
	go run main.go

build:
	docker-compose rm -fv &&\
	docker build . -t read-clickhouse -f Dockerfile --label read-clickhouse

up:
	docker-compose up

ps:
	docker ps
