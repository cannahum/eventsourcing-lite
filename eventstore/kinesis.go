package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

// EventData is the object that gets passed in Kinesis
type EventData struct {
	ApproximateCreationDateTime interface{}
	Keys                        interface{}
	NewImage                    interface{}
}

// ExtractEventDataFromRecord - TODO
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
