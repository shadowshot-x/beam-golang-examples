package main

import "github.com/shadowshot-x/blog-dataflow-flex-golang/pubsubbq"

func main() {
	projectId := "sharmaujjwal-sce"
	pubsub_topic := "pubsub-test"
	pubsub_subscription := "pubsub-test-sub"
	bq_table_string := "sharmaujjwal-sce:test_dataset.test_table"

	pubsubbq.PubsubToBigQuery(projectId, pubsub_topic, pubsub_subscription, bq_table_string)
}
