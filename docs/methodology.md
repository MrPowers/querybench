# QueryBench Methodology

QueryBench provides many types of benchmarks to account for tradeoffs with different engines.

Here are some guiding principles:

1. show the good and bad side of each engine
1. use entirely different query types to show edge case performance (e.g. show group by aggregations on a single low cardinarly column and on many high cardinality columns)
1. build benchmarks that are easy to reproduce
2. benchmark total runtime, including time it takes to load into memory
3. don't allow a single query to bias the overall results

QueryBench uses benchmarking methdology that's fair for all engines.

The queries are designed to highlight the strenghts and weaknesses of engines.

The methodology is designed to level-out obvious biases found in other benchmarks.  Let's start by looking at some of the queries.

## Query selection

It's good to stress test a query engine with different types of queries:

* aggregations on low and high cardinality columns
* aggregations with one group by column and many group by columns
* joins with a large table + a small table and two large tables
* sorting operations on low and high cardinality columns
* compound queries with many operations like a join, then a filter, then a sort

You ideally want to select a representative basket of queries.  This is a great way to expose opportunities for optimizations in engines:

* one engine may need to get better when running group by queries on lots of columns
* another engine may struggle when joining two large engines
* a third engine may need a better optimizer for compound queries

The DataFusion community has successfully used this iterative process to make their engine even faster!

## In memory queries vs. cold queries

There are two different ways to run queries:

* in-memory: load all the data into memory and then quantify the runtime after the data is already loaded
* cold queries: the query runtime to load data from disk and execute the query

Either methodology can be a better representation of your query pattern, depending on how you analyze data.  If you have data loaded in a data warehouse and run queries, then in-memory queries are more representative.  If you store data in cloud storage and run ad hoc queries, then cold queries are more representative.

It's best to present the runtimes for both types of queries to give the most accurate reprensetaion of runtimes.

## Dataset sizes

It's good to show query runtimes for different scale factors to show how engines perform under different loads.  Here are good dataset sizes:

* tiny tables
* medium size tables that easily fit into memory
* large tables that can't fit into memory and really challenge the engine
* larger than memory datasets to see if an engine can handle out-of-memory engines

Distributed engines should be benchmarked with large dataset sizes that can't fit on a single machine.

Some other benchmarking studies only use small scale factors on a single machine for distributed machines that are meant to be run on huge tables, which is misleading.

Make sure you use table sizes that make sense for your hardware specs and query engine.

## File formats

Benchmarks can be built with different file formats:

* Row-based formats like CSV, JSON, or Avro
* Columnar file formats like Parquet and ORC

Some benchmarks use row-based formats which can force columns unrelated to the query to needlessly be loaded into memory.  These benchmarks can end up spending more time testing an engine's CSV reader more than the query execution capabilities.

It's generally better to use columnar file formats like Parquet to better measure an engine's query execution capabilities.

## Lazy vs. eager computations

Some engines execute queries eagerly whereas other lazy engines defer computations till the last possible moment when the user needs results.

pandas is an eager engine and DataFusion is a lazy engine for example.

You need to make sure to execute all the queries with the engines for apples;apples comparisons of the engines.

## Single node vs. distributed compute engines

Some engines are designed to be run on a single machine and others are distributed engines meant to be run on a cluster.

Comparing single machine engines and distributed engines is somewhat pointless:

* distributed engines have lots of overhead and will obviously run slower than single machine engines on a small dataset
* single machine engines will obviously error out for huge datasets that require a cluster

It may be cool to run some distributed engine queries on a single machine, perhaps if you're curious about how your Apache Spark test suite will run locally.

But don't make the mistake of extrapolating Apache Spark vs. pandas benchmarks on a small dataset on a single machine to other dataset sizes.

## Using the same methodology for all engines

Query engines/databases/data warehouses are used and architected quite differently, so they clearly need different benchmarking methodologies.

If your data warehouse always has data loaded in memory, then you can just benchmark the hot queries.

If you only run ad hoc queries for data in cloud storage, then cold run queries are the most useful.

QueryBench aims to display the methodolgy that's most reliable for the different types of query engines.

## OLAP vs. OLTP engines

An engine that's optimized for OLAP queries typically cannot perform OLTP queries as quickly.

A set of queries that simply shows an OLAP engine can perform OLAP queries faster then a OLTP engine isn't particularily interesting.

It's better to show how one engine can perform the OLAP queries faster, but the other excels at OLTP workflows.
