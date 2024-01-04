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

func constructPKCounter(DBitems []map[string]*dynamodb.AttributeValue) map[string]int {

	partitionKeyCount := make(map[string]int)

	for _, i := range DBitems {
		item := Item{}

		err := dynamodbattribute.UnmarshalMap(i, &item)
		taskId := item.TaskId
		if val, ok := partitionKeyCount[taskId]; ok {
			partitionKeyCount[taskId] = (val + 1)
		} else {
			partitionKeyCount[taskId] = 1
		}

		if err != nil {
			log.Fatalf("Got error unmarshalling: %s", err)
		}
	}

	return partitionKeyCount

}

type scanDBData struct {
	itemCount         int64
	lastEvaluatedKey  map[string]*dynamodb.AttributeValue
	items             []map[string]*dynamodb.AttributeValue
	partitionKeyCount map[string]int
}

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

	pkc := constructPKCounter(result.Items)

	return scanDBData{
		itemCount:         *result.Count,
		lastEvaluatedKey:  result.LastEvaluatedKey,
		items:             result.Items,
		partitionKeyCount: pkc,
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
	TaskId string `json:"logGroup"`
}

func fullTableScan(p FTSParams) (int, map[string]int) {
	itemCount := 0
	lastEvalKey := make(map[string]*dynamodb.AttributeValue)
	beforeTime := time.Now()
	fullPKCount := make(map[string]int)

	for {
		scanData := ScanDB(SDBParams{p.svc, p.tableName, lastEvalKey, p.segement, p.totalSegments})
		lastEvalKey = scanData.lastEvaluatedKey
		itemCount = itemCount + int(scanData.itemCount)

		// go through all the values in scanData.partitionKeyCount and check if present, iterate if so, add if not
		for item, count := range scanData.partitionKeyCount {
			if _, ok := fullPKCount[item]; ok {
				fullPKCount[item] = (count + fullPKCount[item])
			} else {
				fullPKCount[item] = count
			}
		}

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
	return itemCount, fullPKCount
}

func ParallelScanDB(svc *dynamodb.DynamoDB, tableName string, totalSegments int64) (int, map[string]int) {
	// show the difference in query time between parallel scan and non parallel scan
	// talk about the eventual consitency of the data

	var totalItemCount int = 0
	beforeTime := time.Now()

	fullParallelPKCount := make(map[string]int)

	// measure time before and after
	var wg sync.WaitGroup

	for i := 0; i < int(totalSegments); i++ {
		wg.Add(1)

		i := i

		go func() {
			defer wg.Done()
			fmt.Printf("Worker %d starting\n", i)
			defer fmt.Printf("Worker %d done\n", i)
			defer fmt.Println(fullParallelPKCount)
			segmentItemCount, pkMap := fullTableScan(FTSParams{svc, tableName, int64(i), totalSegments})
			totalItemCount = totalItemCount + segmentItemCount

			// go through all the values in scanData.partitionKeyCount and check if present, iterate if so, add if not
			for item, count := range pkMap {
				if _, ok := fullParallelPKCount[item]; ok {
					fullParallelPKCount[item] = (count + fullParallelPKCount[item])
				} else {
					fullParallelPKCount[item] = count
				}
			}

		}()
	}

	wg.Wait()

	afterTime := time.Now()
	totalTimeTaken := afterTime.Sub(beforeTime)
	fmt.Printf("Time Taken for parallel scan: %s \n", tableName)
	fmt.Println(totalTimeTaken)
	fmt.Println(fullParallelPKCount)

	return totalItemCount, fullParallelPKCount
}

func main() {
	fmt.Println("start")
	svc := connection()
	tableToScan := "test-cec-aggregated-logs"
	ParallelScanDB(svc, tableToScan, 20)
}
