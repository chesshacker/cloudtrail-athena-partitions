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
	sql    string
}

func main() {
	inputs, err := getProgramInputs()
	checkError(err)
	processor, err := newBucketProcessor(inputs)
	checkError(err)
	err = processor.findOrg()
	checkError(err)
	err = processor.processAccounts()
	checkError(err)
	processor.createAthenaTable()
	fmt.Println(processor.sql)
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
	result.sql = "ALTER TABLE cloudtrail_logs ADD IF NOT EXISTS"
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
			p.prefix += id + "/"
		}
	}
	if !foundId {
		return errors.New("Could not find org id in bucket")
	}
	return nil
}

func (p *bucketProcessor) processAccounts() error {
	accounts, err := p.listFromBucket(p.prefix)
	if err != nil {
		return err
	}
	for _, account := range accounts {
		err := p.processRegion(account)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *bucketProcessor) processRegion(account string) error {
	prefix := p.prefix + account + "/CloudTrail/"
	regions, err := p.listFromBucket(prefix)
	if err != nil {
		return err
	}
	for _, region := range regions {
		err = p.processYear(account, region)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *bucketProcessor) processYear(account, region string) error {
	prefix := p.prefix + account + "/CloudTrail/" + region + "/"
	years, err := p.listFromBucket(prefix)
	if err != nil {
		return err
	}
	for _, year := range years {
		err = p.processMonth(account, region, year)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *bucketProcessor) processMonth(account, region, year string) error {
	prefix := p.prefix + account + "/CloudTrail/" + region + "/" + year + "/"
	months, err := p.listFromBucket(prefix)
	if err != nil {
		return err
	}
	for _, month := range months {
		p.sql += "\nPARTITION (account='" + account + "', region='" + region + "', year='" + year + "', month='" + month + "') LOCATION 's3://" + p.bucket + "/" + p.prefix + account + "/CloudTrail/" + region + "/" + year + "/" + month + "'"
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
			withoutSlash := withoutPrefix[0 : len(withoutPrefix)-1]
			result = append(result, withoutSlash)
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

func (p *bucketProcessor) createAthenaTable() {
	// TODO: actually create the table and return any errors
	fmt.Printf(`
CREATE EXTERNAL TABLE cloudtrail_logs (
	eventversion STRING,
	useridentity STRUCT<
		type:STRING,
		principalid:STRING,
		arn:STRING,
		accountid:STRING,
		invokedby:STRING,
		accesskeyid:STRING,
		userName:STRING,
		sessioncontext:STRUCT<
			attributes:STRUCT<
				mfaauthenticated:STRING,
				creationdate:STRING>,
			sessionissuer:STRUCT<
				type:STRING,
				principalId:STRING,
				arn:STRING,
				accountId:STRING,
				userName:STRING>>>,
	eventtime STRING,
	eventsource STRING,
	eventname STRING,
	awsregion STRING,
	sourceipaddress STRING,
	useragent STRING,
	errorcode STRING,
	errormessage STRING,
	requestparameters STRING,
	responseelements STRING,
	additionaleventdata STRING,
	requestid STRING,
	eventid STRING,
	resources ARRAY<STRUCT<
		ARN:STRING,
		accountId:STRING,
		type:STRING>>,
	eventtype STRING,
	apiversion STRING,
	readonly STRING,
	recipientaccountid STRING,
	serviceeventdetails STRING,
	sharedeventid STRING,
	vpcendpointid STRING
)
PARTITIONED BY (account string, region string, year string, month string)
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://%s/%s';
`, p.bucket, p.prefix)
}
