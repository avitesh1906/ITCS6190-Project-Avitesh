# Structured Streaming Simulation

## Purpose

The NYC TLC Yellow Taxi dataset is historical/static. To satisfy the streaming requirement, this project simulates real-time taxi trip events using Spark Structured Streaming.

## Local Windows Note

The original plan was to simulate streaming by monitoring a landing folder of Parquet files. During local validation on Windows, Spark/Hadoop failed while reading from local Parquet folders because of a Hadoop native library issue:

```text
java.lang.UnsatisfiedLinkError:
'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'