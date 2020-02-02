# CloudTrail Athena Partitions Tool

This tool will help to create partitions in an Athena table based on CloudTrail
logs. This is useful for limiting the amout of data Athena must query.

As a first step, it's helpful to be able to list "directories" in a bucket up to
a certain depth.

```
go run . --bucket your-cloudtrail-bucket-name --prefix "AWSLogs/" --depth 3
```
