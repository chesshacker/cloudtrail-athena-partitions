# CloudTrail Athena Partitions Tool

This tool will help to create partitions in an Athena table based on CloudTrail
logs. This is useful for limiting the amout of data Athena must query.

For instance, I have cloudtrail logs saved to a bucket called
ortizaggies-org-cloudtrail, and I have a different bucket to store athena query
results, called ortizaggies-org-athena-results. I would run:

```
go run . --cloudtrail ortizaggies-org-cloudtrail --athena-results ortizaggies-org-athena-results --year 2020 --month 03
```
