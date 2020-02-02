package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func exitError(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

func listAtDepth(svc *s3.S3, bucket, prefix *string, depth int) {
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket:    bucket,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(50),
		Prefix:    prefix,
	}, func(result *s3.ListObjectsOutput, _ bool) bool {
		for _, p := range result.CommonPrefixes {
			if depth == 1 {
				fmt.Println(*p.Prefix)
			} else {
				listAtDepth(svc, bucket, p.Prefix, depth-1)
			}
		}
		return true
	})
	if err != nil {
		exitError(err)
	}
}

func main() {
	var bucket, prefix string
	var depth int
	flag.StringVar(&bucket, "bucket", "", "AWS bucket name")
	flag.StringVar(&prefix, "prefix", "AWSLogs/", "path prefix")
	flag.IntVar(&depth, "depth", 1, "depth to list files")
	flag.Parse()

	sess, err := session.NewSession()
	if err != nil {
		exitError(err)
	}
	svc := s3.New(sess)
	listAtDepth(svc, aws.String(bucket), aws.String(prefix), depth)
}
