.PHONY: install etl run run-api run-web test

install:
	python -m pip install -r requirements.txt
	cd apps/web && npm install

etl:
	python scripts/run_etl.py

run-api:
	uvicorn apps.api.app.main:app --host 0.0.0.0 --port 8000 --reload

run-web:
	cd apps/web && npm run dev -- --port 3000

run:
	@echo "Ejecuta en dos terminales: make run-api y make run-web"

test:
	pytest -q
