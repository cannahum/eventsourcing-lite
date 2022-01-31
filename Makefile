lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

test:
	go test ./...

coverage:
	go test ./... -coverprofile c.out

local_dynamodb:
	podman run -p 8000:8000 amazon/dynamodb-local
