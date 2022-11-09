deploy: .env docker-compose.yaml
	docker-compose up airflow-init
	docker-compose up -d

cleaning:
	docker-compose down --volumes

cleaningall:
	docker-compose down --volumes --rmi all

warehouse_up:
	docker-compose -f docker-compose-pg-only.yaml up -d

warehouse_down:
	docker-compose -f docker-compose-pg-only.yaml down --volumes
