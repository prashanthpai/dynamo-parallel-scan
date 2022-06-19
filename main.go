package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("os.Open() failed, %v", err)
	}
	defer f.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
	)
	if err != nil {
		log.Fatalf("config.LoadDefaultConfig() failed, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	var gc uint64

	wg := &sync.WaitGroup{}
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func(input dynamodb.ScanInput, segment int) {
			defer wg.Done()

			input.Segment = aws.Int32(int32(segment))
			scanWorker(ctx, client, input, f, &gc)
		}(scanInput, i)
	}

	wg.Wait()
}

func scanWorker(ctx context.Context, client *dynamodb.Client, input dynamodb.ScanInput, f io.Writer, gc *uint64) {
	scanner := dynamodb.NewScanPaginator(client, &input)
	for scanner.HasMorePages() {
		resp, err := scanner.NextPage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("scanner.NextPage() failed, %v", err)
			continue
		}
		atomic.AddUint64(gc, uint64(resp.ScannedCount))

		rows := extractAttributes(resp.Items)
		for _, row := range rows {
			fmt.Fprintf(f, "%s\n", strings.Join(row, ","))
		}

		fmt.Printf("\033[2K\rTotalScanned=%d; Segment=%d; ScannedCount=%d; Count=%d; Rows=%d ",
			atomic.LoadUint64(gc), *input.Segment, resp.ScannedCount, resp.Count, len(rows))
	}
}
