DC = docker compose --env-file=.env --env-file=.env.local

default: up

up:
	@ bash -c 'if [ ! -f ".env.local" ] ; then cp .env.local.dist .env.local; fi;'
	@$(DC) up

down:
	@$(DC) down

build:
	@$(DC) down -v --remove-orphans
	@$(DC) rm -vsf
	@$(DC) up -d --build

list:
	docker ps
