# CloudTrail Athena Partitions Tool

This tool will help to create partitions in an Athena table based on CloudTrail
logs. This is useful for limiting the amout of data Athena must query.

As a first step, it's just creating the statements you need in Athena. Next it
will actually create the tables.

```
go run . --bucket your-cloudtrail-bucket-name
```
