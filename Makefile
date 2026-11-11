.PHONY: start stop

start:
	docker compose up --build -d
	@echo "Page web : http://localhost:8003"

stop:
	docker compose down
