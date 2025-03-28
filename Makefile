lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

test:
	go test ./...

test_local:
	godotenv -f ./.env go test ./...

coverage:
	go test ./... -coverprofile c.out

coverage_local:
	godotenv -f ./.env go test ./...

compose_local_env:
	docker compose -f local/docker-compose.yaml up -d

tear_down_local_env:
	docker compose -f local/docker-compose.yaml down
