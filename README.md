# eventsourcing-lite
## Light-weight Event Sourcing capability with AWS DynamoDB support

### Installation
```bash
go get github.com/cannahum/eventsourcing-lite
```

### Usage
```bash
TODO
```

If you'd like to run tests, make sure to run:
```bash
make docker-dev
```

You will need a `.env` file with at least the following items:
```
AWSCONFIG_REGION="us-west-1" <or your account region>
AWSCONFIG_DYNAMODB_ENDPOINT="http://localhost:8000"
AWSCONFIG_ACCESS_KEY_ID=<only if you want to test in AWS>
AWSCONFIG_SECRET_ACCESS_KEY=<only if you want to test in AWS>
```

Your IntelliJ (or Goland) should pick up on a couple of test / debug configurations. Enjoy...