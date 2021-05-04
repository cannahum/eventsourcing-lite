docker-dev:
	docker-compose -f ./docker-compose.yml up -d

lint:
	golint ./...

fmt:
	go fmt ./...