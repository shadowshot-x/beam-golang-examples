package pubsubbq

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

// Pre - Requisite
// 1. PubSub topic and subscription must be created.
// 2. Dataflow and related APIs should be there.
// 3. Dataflow Admin and Dataflow Worker permission should be assigned to SA.
// 4. BigQuery dataset and table with schema must be created.

// Use Case:-
// 1. Using Golang Apache Beam SDK with Dataflow, Pubsub, BigQuery
// 2. We are getting sensor data from IoT devices to PubSub in the form {device_num}:{cluster_num}-{value_num}
// 3. We need to analyze the data for only 2 cluster numbers.
// 4. For cluster1, values are in per minute. These need to be converted to per second ie. multiply by 60.
// 5. We need to add these to BQ Table grouped by device number with values separated by comma along with append time. This needs to be done in time window of 10 seconds.

type (
	BQRow struct {
		Device string
		Values string
		Time   string
	}
)

func PubsubToBigQuery(projectId, pubsub_topic, pubsub_subscription, bq_table_string string) {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p := beam.NewPipeline()
	s := p.Root()

	pubsub_messages := pubsubio.Read(s, projectId, pubsub_topic, &pubsubio.ReadOptions{
		Subscription: pubsub_subscription,
	})

	// Data Enrichment by appending the existing date to the pubsub message
	// Data Filtering by sending data to different pardo based on data values.
	messages_cluster1, messages_cluster2 := beam.ParDo2(s, func(b []byte, pCollection1, pCollection2 func(string)) {
		msg := string(b)
		if strings.Contains(msg, "cluster1") {
			pCollection1(msg)
		} else if strings.Contains(msg, "cluster2") {
			pCollection2(msg)
		}
	}, pubsub_messages)

	// Data Correction
	cluster1_kv_pair := beam.ParDo(s, func(ss string) (string, string) {
		z := strings.Split(ss, ":")
		if len(z) == 1 {
			return "device1", z[0]
		}
		zz := strings.Split(ss, "-")[1]

		value, err := strconv.Atoi(zz)
		if err != nil {

		}
		value = value * 60
		return z[0], strings.Split(ss, "-")[1] + "-" + fmt.Sprintf("%v", value)
	}, messages_cluster1)

	cluster2_kv_pair := beam.ParDo(s, func(ss string) (string, string) {
		z := strings.Split(ss, ":")
		if len(z) == 1 {
			return "device2", z[0]
		}
		return z[0], z[1]
	}, messages_cluster2)

	// Windowing of 10 seconds added.
	windowed_cluster1 := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), cluster1_kv_pair)
	windowed_cluster2 := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), cluster2_kv_pair)

	// Group by using keys and values
	pCollection_GBK := beam.CoGroupByKey(s, windowed_cluster1, windowed_cluster2)

	// Run the aggregation based on keys for the windowed input
	// This will run every 10 seconds
	key_aggregation := beam.ParDo(s, func(key string, value1, value2 func(*string) bool) string {
		var s_z string
		var v1_list, v2_list string
		for value1(&s_z) {
			v1_list = v1_list + " , " + strings.Split(s_z, "-")[1]
		}
		for value2(&s_z) {
			v2_list = v2_list + " , " + strings.Split(s_z, "-")[1]
		}
		return key + " :::: " + v1_list + v2_list

	}, pCollection_GBK)

	// Create the BQ Row PCollection by processing the output
	create_BQRow := beam.ParDo(s, func(ss string) BQRow {
		z := strings.Split(ss, "::::")
		return BQRow{
			Device: z[0],
			Values: z[1],
			Time:   time.Now().String(),
		}
	}, key_aggregation)

	// print the output
	debug.Print(s, key_aggregation)

	// write to bigquery
	bigqueryio.Write(s, projectId, bq_table_string, create_BQRow)

	// Run the beam pipeline
	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}

}
