# Parallel DB scan

- create a session:
```
gobbc aws-credentials -account 403471490890
```
- run the programme with:
```
go run scan.go
```

NEXT:
- build an object with data on the partition keys and a count of their different values:
  - Extend ScanDB to return a partition key counter map
  - Same with ParallelScanDB - but merge maps from each fullTableScan


## Findings
### int-cec-aggregated-logs
- This table never gets much quicker than 2mins to scan since one segement always has 200k + records on it.
- Run one: 213958
- Run two: 214284
- Why is this? Something to do with partition key values
  - https://www.reddit.com/r/aws/comments/po6pmt/dynamodb_parallel_segment_scan_resulting_in_one/
  - possibly that 50% of the data has the same partition key?
  - build an object with data on the partition keys and a count of their different values


### test-cec-engine-switchboard
- Segment sizes are very even.
- The partition key is a task id, therefore it is unique and there will be an even spread of data across partitions

## Talk Prep Link
- https://paper.dropbox.com/doc/Databases-how-to-read-and-write-data-efficiently--CG44LEkEl2OSMhTcsZxcuaPfAg-DxDDIZREyqDWIipd14dbR

