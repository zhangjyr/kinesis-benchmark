package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"os"
	"os/signal"
	// "strconv"
	"syscall"
	"time"
)

var (
	log = &logger.ColorLogger{ Level: logger.LOG_LEVEL_ALL, Color: true, Verbose: true }
)

type Options struct {
	streamName    string
	regionName    string
}

func main() {
	options := &Options{}
	checkUsage(options)

	// Register signals
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGABRT)

	sess := awsSession.Must(awsSession.NewSession())
	client := kinesis.New(sess, aws.NewConfig().WithRegion(options.regionName))

	// Validate that the stream exists and is active
	stream := validateStream(client, options.streamName);

	// Repeatedly send stock trades with a 100 milliseconds wait in between
	for {
		select {
		case <-sig:
			log.Info("Interrupted, assuming shutdown.");
			os.Exit(0)
		default:
	    getRecord(stream.Shards[0], client, options.streamName);
	    time.Sleep(100 * time.Millisecond)
		}
	}
}

func checkUsage(options *Options) {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	flag.StringVar(&options.regionName, "region", "us-east-1", "AWS region")

	flag.Parse()

	if printInfo || flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: ./bench [options] stream-name\n")
		fmt.Fprintf(os.Stderr, "Available options:\n")
		flag.PrintDefaults()
		os.Exit(0);
	}

	options.streamName = flag.Arg(0)
}

/**
 * Checks if the stream exists and is active
 *
 * @param kinesisClient Amazon Kinesis client instance
 * @param streamName Name of stream
 */
func validateStream(client *kinesis.Kinesis, streamName string) *kinesis.StreamDescription {
	req, resp := client.DescribeStreamRequest(&kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	err := req.Send()
	if err != nil {
		log.Error("Error found while describing the stream %s", streamName)
		os.Exit(1)
	}

	// resp is now filled
	if *resp.StreamDescription.StreamStatus != "ACTIVE" {
		log.Error("Stream %s is not active. Please wait a few moments and try again.", streamName)
		os.Exit(1)
	}

	return resp.StreamDescription
}

func getRecord(shard *kinesis.Shard, client *kinesis.Kinesis, streamName string) []byte {
	start := time.Now()
	iterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId: shard.ShardId,  // We use the size of blob as the partition key.
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeLatest),
		StreamName: aws.String(streamName),
	})
	if err != nil {
		log.Error("Failed to get latest iterator. %v", err);
		os.Exit(1)
	}

	records, err := client.GetRecords(&kinesis.GetRecordsInput{
		Limit: aws.Int64(1),
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		log.Error("Failed to get latest records. %v", err);
		return nil
	}
	dt := time.Since(start)
	log.Debug("Get %s: %v(%d)", *records.Records[0].SequenceNumber, dt, dt)
	return records.Records[0].Data
}
