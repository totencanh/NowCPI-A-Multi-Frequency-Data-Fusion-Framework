
.PHONY: up down ps logs airflow-up airflow-down bronze-cpi dbt-debug

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

airflow-up:
	docker compose --profile airflow up -d --build

airflow-down:
	docker compose --profile airflow down

bronze-cpi:
	docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/app/spark/bronze/cpi.py

dbt-debug:
	cd infra/dbt && dbt debug --profiles-dir .
