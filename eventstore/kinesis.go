package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type EventData struct {
	ApproximateCreationDateTime interface{}
	Keys                        interface{}
	NewImage                    interface{}
}

func ExtractEventDataFromRecord(r kinesis.Record) (EventData, error) {
	m := make(map[string]interface{})
	if parseErr := json.Unmarshal(r.Data, &m); parseErr != nil {
		return EventData{}, parseErr
	}

	if ddbInfoWrapper, ok := m["dynamodb"]; !ok {
		return EventData{}, fmt.Errorf("could not parse %v", m)
	} else if ddbInfo, isTypeMap := ddbInfoWrapper.(map[string]interface{}); !isTypeMap {
		return EventData{}, errors.New("not a map[string]interface{}")
	} else {
		return EventData{
			ApproximateCreationDateTime: ddbInfo["ApproximateCreationDateTime"],
			Keys:                        ddbInfo["Keys"],
			NewImage:                    ddbInfo["NewImage"],
		}, nil
	}
}
