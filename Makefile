run-db:
	docker run --name taxifare_postgres -p 5432:5432 -e POSTGRES_PASSWORD=${DATABASE_PASS} -e POSTGRES_DB=python_db -v ${PWD}/db_data:/var/lib/postgresql/data -d ${DATABASE_USER}