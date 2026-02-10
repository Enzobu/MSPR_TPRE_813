.DEFAULT_GOAL := up

.PHONY: up down exec-etl exec-db logs logs-etl logs-db logs-pgadmin ps

up:
	@docker compose up -d

down:
	@docker compose down

exec-etl:
	@docker compose exec etl bash

exec-db:
	@docker compose exec postgres bash

logs:
	@docker compose logs -f

logs-etl:
	@docker compose logs -f etl

logs-db:
	@docker compose logs -f postgres

logs-pgadmin:
	@docker compose logs -f pgadmin4

ps:
	@docker compose ps
