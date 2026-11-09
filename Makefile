.PHONY: build up down logs ps normal burst duplicate late invalid country rule-update results test clean

build:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f job-submitter jobmanager taskmanager

ps:
	docker compose ps

normal burst duplicate late invalid country rule-update:
	docker compose run --rm scenario-producer $@

results:
	docker compose run --rm topic-reader

test:
	docker build --target builder -t flink-banking-fraud-lab:test .
