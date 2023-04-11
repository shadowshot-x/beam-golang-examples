package main

import (
	"context"
	"flag"
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
// 4. BigQuery dataset and table must be created.

type (
	BQRow struct {
		Table  string
		Datum1 string
		Datum2 string
	}
)

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p := beam.NewPipeline()
	s := p.Root()

	pCollection0 := pubsubio.Read(s, "sharmaujjwal-sce", "pubsub-test", &pubsubio.ReadOptions{
		Subscription: "pubsub-test-sub",
	})

	// Data Enrichment by appending the existing date to the pubsub message
	// Data Filtering by sending data to different pardo based on data values.
	pCollection1, pCollection2 := beam.ParDo2(s, func(b []byte, pCollection1, pCollection2 func(string)) {
		msg := string(b)
		if strings.Contains(msg, "data1") {
			pCollection1(msg + "-" + time.Now().String())
		} else if strings.Contains(msg, "data2") {
			pCollection2(msg + "-" + time.Now().String())
		}
	}, pCollection0)

	// Data Cleaning and Correction
	pCollection_datum_1 := beam.ParDo(s, func(ss string) (string, string) {
		z := strings.Split(ss, ":")
		if len(z) == 1 {
			return "table1", z[0]
		}
		return z[0], strings.Replace(z[1], "ta", "tum", 1)
	}, pCollection1)

	pCollection_datum_2 := beam.ParDo(s, func(ss string) (string, string) {
		z := strings.Split(ss, ":")
		if len(z) == 1 {
			return "table2", z[0]
		}
		return z[0], z[1]
	}, pCollection2)

	// Windowing of 10 seconds added.
	windowedLines_tableName := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), pCollection_datum_1)
	windowedLines_data := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), pCollection_datum_2)

	// Group by using keys and values
	pCollection_GBK := beam.CoGroupByKey(s, windowedLines_tableName, windowedLines_data)

	// Run the aggregation based on keys for the windowed input
	// This will run every 10 seconds
	pCollection_key_aggregation := beam.ParDo(s, func(key string, value1, value2 func(*string) bool) string {
		var s_z string
		var v1_list, v2_list string
		for value1(&s_z) {
			v1_list = v1_list + " , " + s_z
		}
		for value2(&s_z) {
			v2_list = v2_list + " , " + s_z
		}
		return key + " :::: " + v1_list + " :::: " + v2_list

	}, pCollection_GBK)

	// Create the BQ Row PCollection by processing the output
	pCollection_BQRow := beam.ParDo(s, func(ss string) BQRow {
		z := strings.Split(ss, "::::")
		return BQRow{
			Table:  z[0],
			Datum1: z[1],
			Datum2: z[2],
		}
	}, pCollection_key_aggregation)

	// print the output
	debug.Print(s, pCollection_key_aggregation)

	// write to bigquery
	bigqueryio.Write(s, "sharmaujjwal-sce", "sharmaujjwal-sce:test_dataset.test_table", pCollection_BQRow)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}

}
