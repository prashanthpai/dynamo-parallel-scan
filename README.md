# dynamo-parallel-scan

dynamo-parallel-scan runs a [parallel scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)
on Amazon DynamoDB to extract attributes matching a filter and stores them in
a file. It uses [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2).

### Usage

Edit [config.go](./config.go) to change table name, scan parameters,
attribute extraction logic etc.

```sh
go build
./dynamo-parallel-scan
```
