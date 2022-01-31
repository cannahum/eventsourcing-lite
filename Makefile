lint:
	golint ./...

fmt:
	go fmt ./...

test:
	go test ./...

local_dynamodb:
	podman run -p 8000:8000 amazon/dynamodb-local
