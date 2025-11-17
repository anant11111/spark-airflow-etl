#!/usr/bin/env python3
"""
spark_job/app.py
Optimized Spark ETL:
 - For each (id, name) keep row with max(timestamp)
 - Aggregate into Map<string,string> per id
 - Write partitioned by dt=<date>
Usage:
  spark-submit spark_job/app.py --input-path /path/to/input/*.parquet --output-path /path/to/out --date 2025-11-16
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, struct, collect_list, expr, lit
from pyspark.sql.window import Window

def build_spark(app_name="settings-etl", shuffle_partitions=None):
    builder = SparkSession.builder.appName(app_name)
    spark = builder.getOrCreate()
    if shuffle_partitions:
        spark.conf.set("spark.sql.shuffle.partitions", int(shuffle_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    # tune file partition size for local dev (can be overridden on cluster)
    spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)
    return spark

def read_input(spark, input_path):
    return spark.read.load(input_path)

def transform(df):
    required = ("id","name","value","timestamp")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Input missing required columns: {missing}")

    df = df.select(
        col("id").cast("long"),
        col("name").cast("string"),
        col("value").cast("string"),
        col("timestamp").cast("long"),
    )

    w = Window.partitionBy(col("id"), col("name")).orderBy(col("timestamp").desc())
    top_per_name = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    entry = struct(col("name").alias("key"), col("value").alias("value"))
    grouped = top_per_name.groupBy("id").agg(collect_list(entry).alias("entries"))

    result = grouped.select(col("id"), expr("map_from_entries(entries) as settings"))
    return result

def write_output(df, output_path, date_str, repartition=200, mode="overwrite"):
    out = df.withColumn("dt", lit(date_str))
    out = out.repartition(int(repartition), col("id"))
    out.write.mode(mode).partitionBy("dt").parquet(output_path)

def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--input-path", required=True)
    p.add_argument("--output-path", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--shuffle-partitions", default=None)
    p.add_argument("--repartition", default=200, type=int)
    p.add_argument("--mode", default="overwrite")
    return p.parse_args(argv)

def main(argv):
    args = parse_args(argv)
    spark = build_spark(shuffle_partitions=args.shuffle_partitions)
    df = read_input(spark, args.input_path)
    out = transform(df)
    write_output(out, args.output_path, args.date, repartition=args.repartition, mode=args.mode)
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
