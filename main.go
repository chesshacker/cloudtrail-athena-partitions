package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type programInputs struct {
	bucket string
}

type bucketProcessor struct {
	svc    *s3.S3
	bucket string
	prefix string
	org    string
}

func main() {
	inputs, err := getProgramInputs()
	checkError(err)
	processor, err := newBucketProcessor(inputs)
	checkError(err)
	err = processor.findOrg()
	checkError(err)
	err = processor.findAccounts()
	checkError(err)
}

func getProgramInputs() (*programInputs, error) {
	var result programInputs
	flag.StringVar(&result.bucket, "bucket", "", "AWS bucket name")
	flag.Parse()
	if result.bucket == "" {
		return nil, errors.New("bucket is a required parameter")
	}
	return &result, nil
}

// bucket_name/prefix_name/AWSLogs/OU-ID/Account-ID/CloudTrail/region/YYYY/MM/DD/file_name.json.gz

func newBucketProcessor(inputs *programInputs) (*bucketProcessor, error) {
	var result bucketProcessor
	result.bucket = inputs.bucket
	result.prefix = "AWSLogs/"
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	result.svc = s3.New(sess)
	return &result, nil
}

func (p *bucketProcessor) findOrg() error {
	foundId := false
	ids, err := p.listFromBucket(p.prefix)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id[0:2] == "o-" {
			foundId = true
			p.prefix += id
		}
	}
	if !foundId {
		return errors.New("Could not find org id in bucket")
	}
	return nil
}

func (p *bucketProcessor) findAccounts() error {
	accounts, err := p.listFromBucket(p.prefix)
	if err != nil {
		return err
	}
	for _, account := range accounts {
		fmt.Println(p.prefix + account)
	}
	return nil
}

func (p *bucketProcessor) listFromBucket(prefix string) ([]string, error) {
	var result []string
	err := p.svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket:    &p.bucket,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(50),
		Prefix:    &prefix,
	}, func(page *s3.ListObjectsOutput, _ bool) bool {
		for _, p := range page.CommonPrefixes {
			withoutPrefix := strings.Replace(*p.Prefix, prefix, "", 1)
			result = append(result, withoutPrefix)
		}
		return true
	})
	return result, err
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
