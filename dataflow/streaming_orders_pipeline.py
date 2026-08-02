"""Streaming order-events pipeline.

Reads order events from Pub/Sub, windows them into one-minute tumbling
windows, aggregates revenue per store, and writes to BigQuery.

Run:
    python dataflow/streaming_orders_pipeline.py \
        --runner=DataflowRunner \
        --project=gcai-may-26 \
        --region=us-central1 \
        --streaming
"""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window

SCHEMA = "store_id:STRING,window_start:TIMESTAMP,order_count:INTEGER,revenue:FLOAT"


class ParseOrder(beam.DoFn):
    """Parse a Pub/Sub payload into (store_id, revenue)."""

    def process(self, element):
        record = json.loads(element.decode("utf-8"))
        yield record["store_id"], float(record["amount"])


class FormatRow(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        store_id, (count, revenue) = element
        yield {
            "store_id": store_id,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "order_count": count,
            "revenue": revenue,
        }


def count_and_sum(values):
    values = list(values)
    return len(values), sum(values)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription",
                        default="projects/gcai-may-26/subscriptions/orders-sub")
    parser.add_argument("--output_table",
                        default="gcai-may-26:sales.store_revenue_1m")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription)
            | "ParseOrder" >> beam.ParDo(ParseOrder())
            | "Window1m" >> beam.WindowInto(window.FixedWindows(60))
            | "GroupByStore" >> beam.GroupByKey()
            | "Aggregate" >> beam.MapTuple(lambda k, v: (k, count_and_sum(v)))
            | "FormatRow" >> beam.ParDo(FormatRow())
            | "WriteBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
