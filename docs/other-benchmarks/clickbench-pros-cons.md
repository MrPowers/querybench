# ClickBench Benchmarks Pros and Cons

The Clickbench benchmarks are based on a dataset with 99,537,185 rows and XX columns.  The `hits.parquet` file is 14.8 GB in storage.

They contain queries like the following:

* Select an average from a column: `SELECT AVG("UserID") FROM hits;`
* Aggregation then sort: `SELECT "UserID", COUNT(*) FROM hits GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10;`
* Pattern matching: `SELECT * FROM hits WHERE "URL" LIKE \'%google%\' ORDER BY "EventTime" LIMIT 10;`

The benchmarks are displayed as a cumulative runtime of all the queries.

Here's an example of the comparison they present for DataFusion vs. Daft:

XXX

And here's the comparison for a select few queries:

XXX

Let's turn our attention to the wonderful benefits of the Clickbench benchmarks.

## Benefits of the ClickBench benchmarks

The Clickbench benchmarks are easy to understand, cover a variety of different query types, and are useful for end users and query engine maintainers.  They make it relatively easy to spot queries where a given engine underperforms.

**Covers lots of different engines**

The Clickbench benchmarks cover many engines, including Daft, Polars, and TODO (add more).

Some engines are missing, notably TODO.

**Good variety of queries**

XXX

**Easily accessible dataset**

The Clickbench dataset can be downloaded in the project README.

This is a significant improvement compared to the h2o dataset, which requires running an R program, which can be challenging if you aren't familiar with the R programming language.

**Data in Parquet files**

The Clickbench dataset is distributed in different file formats, including Parquet, which is ideal for benchmarks. Parquet is columnar, allowing for column pruning, has a well-defined schema, so engines don't need to infer column names or types, and is binary, making storage files on disk relatively small.

File formats like CSV can cause complications when comparing two engines.

**Cover hot and cold query times**

The Clickbench queries include query runtimes when data is already loaded in memory and when data is on disk and not loaded yet.

This provides a good representation of runtimes for databases when data is already loaded, as well as for enterprises that run ad hoc queries on data stored on disk.

**Different machine sizes are used**

XXX

## Disadvantages of the Clickbench benchmarks

The Clickbench benchmarks are excellent, but don't test important types of queries and have many flaws.

Let's review the limitations of Clickbench so that you can interpret the results.

**No join queries**

The Clickbench queries only cover single-table queries and don't consider joins or compound queries with joins.

Joins are often the biggest bottleneck for a query, so this is a significant omission.

**Only one dataset size**

All the Clickhouse benchmarks are run on the table with XX rows.

The benchmarks aren't run on datasets with different scale factors.  It'd be interesting to see the same benchmarks run on the same table with XX rows, XX rows, etc.

This could stress test engines that have issues when the load stresses the limits of the hardware for a machine.

**One slow query has a significant impact on results**

The query times for all 42 queries are presented in total, which means that a single slow-running query can greatly impact the overall result. Ranking the queries based on their ranking for all queries would be another reliable way to present the results.

**Not useful for specialized query engines**

Some of the query engines in Clickbench are specialized for a use case and aren't optimized general-purpose engines:

* Time is for time series analyses
* XX is for OLTP
* XX is for XX

It's better to benchmark specialized query engines with specialized query patterns.  For example, a spatial query engine should be benchmarked using spatial queries and compared with other spatial engines.

**Bad for distributed query engines**

Properly testing a distributed engine requires large datasets spread across many files and a computation cluster with several machines.

The Clickbench dataset is small, and the queries can be run on a single laptop with only 16 GB of memory.  It's way too small of a dataset to test a distributed engine.

The Clickbench dataset could be 10 times larger and that would give an interesting 147.8 GB dataset to test a distributed engine.

**Some metadata-only queries**

The first query can be computed from the Parquet metadata alone: `"SELECT COUNT(*) FROM hits;"`.

The query engine can fetch the row count from the metadata of every row group in the file and compute the sum.

It's essential to verify that engines utilize metadata to run queries faster when possible, but it's not particularly interesting after that has been confirmed.

**Lots of quick queries**

A large portion of the queries (around 26) can be run in under a second on a MacBook M3 with 16 GB of RAM.  

Ten queries finish between 1 and 3 seconds.

The remaining six queries take more than 3 seconds to run.

The queries are skewed to lightweight workflows that can run very quickly.

## Conclusion

Querybench is an excellent benchmarking analysis and a significant contribution to the data community.

It's fantastic that Clickhouse has open-sourced the dataset and the code.  It's also amazing that they maintain the benchmarks and made a nice user interface.
