# Apache Beam Golang Examples

## Example 1
Dataflow Runner : PubSub to BigQuery using coGroupbyKey, pubsubio and bigqueryio

### Instructions to Run
```
go run main.go --project=sharmaujjwal-sce --runner=dataflow --region=us-west1 --staging_location=gs://dataflow-staging-us-west1-d46d1a8427a48a6e260f0359f8fe8762/check --service_account_email=sa-hbase-bt-migration-poc@sharmaujjwal-sce.iam.gserviceaccount.com
```