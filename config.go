package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	awsRegion   = "ap-southeast-1"
	tableName   = "table_name_goes_here"
	outputFile  = "pks.txt"
	threadCount = 10
)

var (
	scanInput = dynamodb.ScanInput{
		TotalSegments:        aws.Int32(threadCount),
		TableName:            aws.String(tableName),
		Select:               types.SelectSpecificAttributes,
		FilterExpression:     aws.String("eat < :num"),
		ProjectionExpression: aws.String("nid, eat"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":num": &types.AttributeValueMemberN{
				Value: "1640995199000", // epoch in ms; 31 December 2021 23:59:59 GMT
			},
		},
	}
)

func extractAttributes(items []map[string]types.AttributeValue) [][]string {
	var rows [][]string

	for _, m := range items {
		v, ok := m["nid"]
		if !ok {
			continue
		}
		pn, ok := v.(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		nid := pn.Value

		v, ok = m["eat"]
		if !ok {
			continue
		}
		pe, ok := v.(*types.AttributeValueMemberN)
		if !ok {
			continue
		}
		t, err := msToTime(pe.Value)
		if err != nil {
			fmt.Println(pe.Value, err.Error())
			continue
		}
		t = t.UTC()
		if t.Year() > 2021 {
			continue
		}

		rows = append(rows, []string{nid})
	}

	return rows
}

func msToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.UnixMilli(msInt), nil
}
