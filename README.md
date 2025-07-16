# MrPowers Benchmarks

This repo performs benchmarking analysis on common datasets with popular query engines like pandas, Polars, DataFusion, and PySpark.  For example, here are the slower ClickBench benchmarks for a few select engines:

![clickbench-slow](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/clickbench-slow.svg)

It draws inspiration from the [h2o benchmarks](https://github.com/h2oai/db-benchmark) but also includes different types of queries and uses some different execution methodologies (e.g. modern file formats).

The h2o benchmarks have been a great contribution to the data community, but they've made some decisions that aren't as relevant for modern workflows, see [this section](https://github.com/MrPowers/mrpowers-benchmarks#h2o-benchmark-methodology) for more details.

Most readers of this repo are interested in the benchmarking results and don't actually want to reproduce the benchmarking results themselves.  In any case, this repo makes it easy for readers to reproducing the results themselves.  This is particularily useful if you'd like to run the benchmarks on a specific set of hardware.

This repo provides clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the actual benchmarks that are run.

## h2o join on localhost

Here are the results for the h2o join queries on the 1e7 dataset:

![h2o_join_1e7](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/h2o-join-1e7.svg)

And here are the results on the 1e8 dataset:

![h2o_join_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/h2o-join-1e8.svg)

## h2o groupby on localhost

Here are the results for the h2o groupby queries on the 10 million row dataset (stored in a single Parquet file):

![fast_h2o_groupby_1e7](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-fast-1e7.svg)

Here are the results for the h2o groupby queries on the 100 million row dataset:

![fast_h2o_groupby_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-fast-1e8.svg)

Here are the longer-running group by queries:

![slow_h2o_groupby_1e7](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-slow-1e7.svg)

Here they are on the bigger dataset:

![slow_h2o_groupby_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-slow-1e8.svg)

## Revised h2o methodology

These queries were run on a Macbook M3 with 16 GB of RAM.

Here's how the benchmarking methdology differs from the h2o benchmarks:

* they include the full query time (h2o benchmarks just include the query time once the data is loaded in memory)
* Parquet files are used instead of CSV

## Single table query results

Here are the results for single table queries on the 1e7 dataset:

![single_table_1e7](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/single-table-1e7.svg)

And here are the results on the 1e8 table: 

![single_table_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/single-table-1e8.svg)

## ClickBench queries

Here are the fast ClickBench queries:

![clickbench-fast.svg](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/clickbench-fast.svg)

Here are the slow ClickBench queries: 

![clickbench-slow.svg](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/clickbench-slow.svg)


