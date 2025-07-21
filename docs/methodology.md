# QueryBench Methodology

QueryBench offers various benchmarks to account for trade-offs with different engines.

Here are some guiding principles:

1. Show the good and bad sides of each engine
1. Use entirely different query types to show edge case performance (e.g., show group by aggregations on a single low cardinality column and many high cardinality columns)
1. Build benchmarks that are easy to reproduce
2. Benchmark total runtime, including the time it takes to load into memory
3. Don't allow a single query to bias the overall results

QueryBench uses a benchmarking methodology that's fair for all engines.

The queries are designed to highlight the strengths and weaknesses of engines.

The methodology is designed to level out obvious biases found in other benchmarks.  Let's begin by examining some of the queries.

## Query selection

It's good to stress test a query engine with different types of queries:

* aggregations on low and high cardinality columns
* aggregations with one group by column and many group by columns
* joins with a large table + a small table and two large tables
* sorting operations on low and high cardinality columns
* compound queries with many operations like a join, then a filter, then a sort

You should select a representative basket of queries.  This is a great way to expose opportunities for optimizations in engines:

* One engine may need to get better when running group-by queries on lots of columns
* Another engine may struggle when joining two large engines
* A third engine may need a better optimizer for compound queries

The DataFusion community has successfully utilized this iterative process to enhance the speed of their engine further.

## In-memory queries vs. cold queries

There are two different ways to run queries:

* in-memory: load all the data into memory and then quantify the runtime after the data is already loaded
* cold queries: the query runtime to load data from disk and execute the query

Either methodology can be a better representation of your query pattern, depending on how you analyze data.  If you have data loaded in a data warehouse and run queries, then in-memory queries are more representative.  If you store data in cloud storage and run ad hoc queries, then cold queries are more representative.

It's best to present the runtimes for both types of queries to give the most accurate representation of runtimes.

## Dataset sizes

It's beneficial to display query runtimes for various scale factors to illustrate how engines perform under different loads.  Here are good dataset sizes:

* tiny tables
* medium-sized tables that easily fit into memory
* large tables that can't fit into memory and challenge the engine
* larger than memory datasets to see if an engine can handle out-of-memory engines

Distributed engines should be benchmarked with large dataset sizes that can't fit on a single machine.

Some other benchmarking studies only use small-scale factors on a single machine for distributed machines that are meant to be run on huge tables, which is misleading.

Ensure you use table sizes that align with your hardware specifications and query engine.

## File formats

Benchmarks can be built with different file formats:

* Row-based formats like CSV, JSON, or Avro
* Columnar file formats like Parquet and ORC

Some benchmarks use row-based formats, which can force columns unrelated to the query to be needlessly loaded into memory.  These benchmarks can end up spending more time testing an engine's CSV reader than the query execution capabilities.

It's generally better to use columnar file formats, such as Parquet, to more accurately measure an engine's query execution capabilities.

## Lazy vs. eager computations

Some engines execute queries eagerly, whereas others execute computations lazily, deferring them until the last possible moment when the user needs the results.

Pandas is an eager engine, and DataFusion is a lazy engine, for example.

Please ensure that you fully execute queries with every engine for a fair comparison.

## Single node vs. distributed compute engines

Some engines are designed to run on a single machine, while others are distributed engines intended to run on a cluster.

Comparing single-machine engines and distributed engines is somewhat pointless:

* Distributed engines have lots of overhead and will run slower than single-machine engines on a small dataset
* Single-machine engines will error out for massive datasets that require a cluster

It may be interesting to run some distributed engine queries on a single machine if you're curious about how your Apache Spark test suite performs locally.

But don't make the mistake of extrapolating Apache Spark vs. pandas benchmarks on a small dataset on a single machine to other dataset sizes.

## Using the same methodology for all engines

Query engines/databases/data warehouses are used and architected quite differently, so they need different benchmarking methodologies.

If your data warehouse always has data loaded in memory, then you can benchmark the hot queries.

If you only run ad hoc queries for data in cloud storage, then cold run queries are the most useful.

QueryBench aims to display the methodology that's most reliable for the different types of query engines.

## OLAP vs. OLTP engines

An engine that's optimized for OLAP queries typically cannot perform OLTP queries as quickly.

A set of queries that shows an OLAP engine can perform OLAP queries faster than an OLTP engine isn't exciting.

It's better to demonstrate how one engine can perform OLAP queries faster, while the other excels at OLTP workflows.
