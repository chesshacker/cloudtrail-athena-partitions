# CloudTrail Athena Partitions Tool

This tool will help to create partitions in an Athena table based on CloudTrail
logs. This is useful for limiting the amout of data Athena must query.

For instance, I have cloudtrail logs saved to a bucket called
ortizaggies-org-cloudtrail, and I have a different bucket to store athena query
results, called ortizaggies-org-athena-results. I could run the following to
create all the partitions:

```
go run . --cloudtrail ortizaggies-org-cloudtrail --athena-results ortizaggies-org-athena-results
```

If I only wanted to create partitions for the current month, I could run:

```
go run . --cloudtrail ortizaggies-org-cloudtrail --athena-results ortizaggies-org-athena-results --current-month
```

If I wanted to create partitions for a specific month, I could run:

```
go run . --cloudtrail ortizaggies-org-cloudtrail --athena-results ortizaggies-org-athena-results --year 2020 --month 03
```
