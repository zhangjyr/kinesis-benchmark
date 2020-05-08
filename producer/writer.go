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
	"strconv"
	"syscall"
	"time"

	"github.com/zhangjyr/kinesis-benchmark/producer/generator"
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
	validateStream(client, options.streamName);

	// Repeatedly send stock trades with a 100 milliseconds wait in between
	gen := &generator.BlobGenerator{}
	for {
		select {
		case <-sig:
			log.Info("Interrupted, assuming shutdown.");
			os.Exit(0)
		default:
	    sendRecord(gen.Get(), client, options.streamName, strconv.FormatInt(time.Now().UnixNano(), 10));
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
		fmt.Fprintf(os.Stderr, "Usage: ./producer [options] stream-name\n")
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
func validateStream(client *kinesis.Kinesis, streamName string) {
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
}

/**
 * Uses the Kinesis client to send the stock trade to the given stream.
 *
 * @param trade instance representing the stock trade
 * @param kinesisClient Amazon Kinesis client
 * @param streamName Name of stream
 */
func sendRecord(blob []byte, client *kinesis.Kinesis, streamName string, seq string) {
	log.Debug("Putting blob: len(%d)", len(blob))
	req, resp := client.PutRecordRequest(&kinesis.PutRecordInput{
		PartitionKey: aws.String(strconv.Itoa(len(blob))),  // We use the size of blob as the partition key.
		StreamName: aws.String(streamName),
		Data: blob,
		SequenceNumberForOrdering: aws.String(seq),
	})
	start := time.Now()
	err := req.Send()
	dt := time.Since(start)
	if err != nil {
		log.Error("Exception while sending data to Kinesis. Will try again next cycle. %v", err);
	} else {
		log.Debug("Sent %d blob: %s,%s,%v,%d", len(blob), *resp.ShardId, *resp.SequenceNumber, dt, dt)
	}
}
