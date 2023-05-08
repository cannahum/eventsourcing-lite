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

local_dynamodb:
	podman run -p 8000:8000 amazon/dynamodb-local
