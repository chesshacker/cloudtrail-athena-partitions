package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
)

type programInputs struct {
	cloudtrail     string
	athena_results string
	year           string
	month          string
}

type processor struct {
	svc            *s3.S3
	ath            *athena.Athena
	cloudtrail     string
	athena_results string
	year           string
	month          string
	prefix         string
	sql            string
}

func main() {
	inputs, err := getProgramInputs()
	checkError(err)
	processor, err := newProcessor(inputs)
	checkError(err)
	err = processor.findOrg()
	checkError(err)
	err = processor.processAccounts()
	checkError(err)
	err = processor.applySql()
	checkError(err)
	fmt.Print("Partitions processed:")
	fmt.Println(processor.sql)
}

func getProgramInputs() (*programInputs, error) {
	var result programInputs
	flag.StringVar(&result.cloudtrail, "cloudtrail", "", "AWS bucket name for cloudtrail logs")
	flag.StringVar(&result.athena_results, "athena-results", "", "AWS bucket name/path to store athena results")
	flag.StringVar(&result.year, "year", "", "year to partition")
	flag.StringVar(&result.month, "month", "", "month to partition")
	flag.Parse()
	if result.cloudtrail == "" {
		return nil, errors.New("bucket is a required parameter")
	}
	if result.athena_results == "" {
		return nil, errors.New("athena-results is a required parameter")
	}
	// year and month are optional
	// TODO: add current-month flag
	return &result, nil
}

// bucket_name/prefix_name/AWSLogs/OU-ID/Account-ID/CloudTrail/region/YYYY/MM/DD/file_name.json.gz

func newProcessor(inputs *programInputs) (*processor, error) {
	var result processor
	result.cloudtrail = inputs.cloudtrail
	result.athena_results = inputs.athena_results
	result.prefix = "AWSLogs/"
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	result.svc = s3.New(sess)
	result.ath = athena.New(sess)
	return &result, nil
}

func (p *processor) findOrg() error {
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

func (p *processor) processAccounts() error {
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

func (p *processor) processRegion(account string) error {
	prefix := p.prefix + account + "/CloudTrail/"
	regions, err := p.listFromBucket(prefix)
	if err != nil {
		return err
	}
	for _, region := range regions {
		if p.year == "" {
			err = p.processYear(account, region)
		} else {
			err = p.processMonth(account, region, p.year)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) processYear(account, region string) error {
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

func (p *processor) processMonth(account, region, year string) error {
	prefix := p.prefix + account + "/CloudTrail/" + region + "/" + year + "/"
	var months []string
	var err error
	if p.month == "" {
		months, err = p.listFromBucket(prefix)
	} else {
		months = []string{p.month}
	}

	if err != nil {
		return err
	}
	for _, month := range months {
		p.sql += fmt.Sprintf("\nPARTITION (account='%s', region='%s', year='%s', month='%s') LOCATION 's3://%s/%s/%s'",
			account, region, year, month, p.cloudtrail, prefix, month)
	}
	return nil
}

func (p *processor) listFromBucket(prefix string) ([]string, error) {
	var result []string
	err := p.svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket:    &p.cloudtrail,
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

func (p *processor) applySql() error {
	sql := p.createTableSql()
	_, err := p.ath.StartQueryExecution(p.getStartQueryExecutionInput(sql))
	if err != nil {
		return err
	}
	sql = "ALTER TABLE cloudtrail_logs ADD IF NOT EXISTS\n" + p.sql
	_, err = p.ath.StartQueryExecution(p.getStartQueryExecutionInput(sql))
	return err
}

func (p *processor) getStartQueryExecutionInput(sql string) *athena.StartQueryExecutionInput {
	return &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("Default"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://" + p.athena_results),
		},
	}
}

func (p *processor) createTableSql() string {
	return fmt.Sprintf(`
CREATE EXTERNAL TABLE IF NOT EXISTS cloudtrail_logs (
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
`, p.cloudtrail, p.prefix)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
