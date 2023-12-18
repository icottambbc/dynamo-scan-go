package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type scanDBData struct {
	itemCount        int64
	lastEvaluatedKey map[string]*dynamodb.AttributeValue
	items            []map[string]*dynamodb.AttributeValue
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
	tlimit := int64(10)

	scanInput := &dynamodb.ScanInput{
		TableName: &p.tableName,
		Limit:     &tlimit,
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

	// constructPKCounter(result.Items)

	return scanDBData{
		itemCount:        *result.Count,
		lastEvaluatedKey: result.LastEvaluatedKey,
		items:            result.Items,
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

type Item struct {
	taskId string
}

func constructPKCounter(DBitems []map[string]*dynamodb.AttributeValue) {
	for _, i := range DBitems {
		item := Item{}

		fmt.Println(i)

		err := dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			log.Fatalf("Got error unmarshalling: %s", err)
		}
	}

}

func fullTableScan(p FTSParams) int {
	itemCount := 0
	lastEvalKey := make(map[string]*dynamodb.AttributeValue)
	beforeTime := time.Now()

	for {
		scanData := ScanDB(SDBParams{p.svc, p.tableName, lastEvalKey, p.segement, p.totalSegments})
		lastEvalKey = scanData.lastEvaluatedKey
		// fmt.Println(lastEvalKey)
		itemCount = itemCount + int(scanData.itemCount)

		moreItems := len(scanData.lastEvaluatedKey) > 0
		if !moreItems {
			break
		}
	}

	afterTime := time.Now()
	totalTimeTaken := afterTime.Sub(beforeTime)

	if p.segement < 0 {
		fmt.Printf("Time Taken for full table scan: %s \n", p.tableName)
	} else {
		fmt.Printf("Time Taken for table scan segement: %s \n", p.tableName)
	}
	fmt.Println(totalTimeTaken)
	fmt.Printf("Segment Item Count: %d \n", itemCount)
	return itemCount
}

func ParallelScanDB(svc *dynamodb.DynamoDB, tableName string, totalSegments int64) int {
	// show the difference in query time between parallel scan and non parallel scan
	// talk about the eventual consitency of the data

	var totalItemCount int = 0
	beforeTime := time.Now()

	// measure time before and after
	var wg sync.WaitGroup

	for i := 0; i < int(totalSegments); i++ {
		wg.Add(1)

		i := i

		go func() {
			defer wg.Done()
			fmt.Printf("Worker %d starting\n", i)
			defer fmt.Printf("Worker %d done\n", i)
			segmentItemCount := fullTableScan(FTSParams{svc, tableName, int64(i), totalSegments})
			totalItemCount = totalItemCount + segmentItemCount
		}()
	}

	wg.Wait()

	afterTime := time.Now()
	totalTimeTaken := afterTime.Sub(beforeTime)
	fmt.Printf("Time Taken for parallel scan: %s \n", tableName)
	fmt.Println(totalTimeTaken)

	return totalItemCount
}

func main() {
	fmt.Println("start")
	svc := connection()
	tableToScan := "test-cec-engine-switchboard"
	itemCount := ParallelScanDB(svc, tableToScan, 20)
	fmt.Printf("Item Count for Parallel Table Scan: %s \n", tableToScan)
	fmt.Printf("%d \n\n", itemCount)
	// ftItemCount := fullTableScan(FTSParams{svc, tableToScan, 0, 20})
	// emptyKey := make(map[string]*dynamodb.AttributeValue)
	// ScanDB(SDBParams{svc, tableToScan, emptyKey, -1, -1})
	// fmt.Printf("Item Count for Full Table Scan: %s \n", tableToScan)
	// fmt.Println(ftItemCount)
}
