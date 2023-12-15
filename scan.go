package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type scanDBData struct {
	itemCount        int64
	lastEvaluatedKey map[string]*dynamodb.AttributeValue
}

// use a while loop - while ScanDB returns a Last evaluated key, do another one and increment the times

type SDBParams struct {
	svc           *dynamodb.DynamoDB
	tableName     string
	lastEvalKey   map[string]*dynamodb.AttributeValue
	segement      int64
	totalSegments int64
}

func ScanDB(p SDBParams) scanDBData {
	scanInput := &dynamodb.ScanInput{
		TableName: &p.tableName,
	}

	if len(p.lastEvalKey) > 0 {
		scanInput.ExclusiveStartKey = p.lastEvalKey
	}

	if p.segement >= 0 && p.totalSegments >= 0 {
		scanInput.Segment = &p.segement
		scanInput.TotalSegments = &p.totalSegments
	}

	// call scan table
	result, err := p.svc.Scan(scanInput)
	if err != nil {
		fmt.Println(err.Error())
	}

	return scanDBData{
		itemCount:        *result.Count,
		lastEvaluatedKey: result.LastEvaluatedKey,
	}
}

func ListDBs(svc *dynamodb.DynamoDB) {
	tableLimit := int64(5)

	input := &dynamodb.ListTablesInput{
		Limit: &tableLimit,
	}

	result, err := svc.ListTables(input)

	if err != nil {
		fmt.Println(err.Error())
	}

	for _, n := range result.TableNames {
		fmt.Println(*n)
	}
}

func connection() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)
	return svc
}

type FTSParams struct {
	svc           *dynamodb.DynamoDB
	tableName     string
	segement      int64
	totalSegments int64
}

func fullTableScan(p FTSParams) int {
	itemCount := 0
	lastEvalKey := make(map[string]*dynamodb.AttributeValue)
	beforeTime := time.Now()

	for {
		scanData := ScanDB(SDBParams{p.svc, p.tableName, lastEvalKey, p.segement, p.totalSegments})
		lastEvalKey = scanData.lastEvaluatedKey
		fmt.Println(lastEvalKey)
		itemCount = itemCount + int(scanData.itemCount)
		moreItems := len(scanData.lastEvaluatedKey) > 0
		if !moreItems {
			break
		}
	}

	afterTime := time.Now()
	totalTimeTaken := afterTime.Sub(beforeTime)

	fmt.Printf("Time Taken for scan - this is a lie: %s \n", p.tableName)
	fmt.Println(totalTimeTaken)
	return itemCount
}

func ParallelScanDB(tableName string, totalSegments int64) {
	// https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.Scan
	// https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#ScanInput
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan

	// show the difference in query time between parallel scan and non parallel scan
	// talk about the eventual consitency of the data

	// call fullTableScan with two additional parameters - segement and totalsegments
	// return item count from full table scan
	// hold a total for all the item counts
	// have a loop that calls fullTable scan on multiple threads using go routines
	// measure time before and after

}

func main() {
	fmt.Println("start")
	svc := connection()
	tableToScan := "test-cec-engine-switchboard"
	itemCount := fullTableScan(FTSParams{svc, tableToScan, 0, 3})
	fmt.Printf("Item Count for table: %s \n", tableToScan)
	fmt.Println(itemCount)
}
