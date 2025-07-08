# QueryBench

QueryBench provides high-quality benchmarks for popular engines, so you can easily identify the best tool for your workloads.

Here's an example of runtimes for some queries on various engines:

![](./images/groupby-fast-1e8.png)

## QueryBench methodology

1. show the good and bad side of each engine
1. use columnar file formats
2. benchmark total runtime, including time it takes to load into memory
3. don't allow a single query to bias the overall results

## Why benchmarks are important

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

Benchmarks can be harmful when they're biased or improperly structured and give misleading conclusions.  Benchmarks should not intentionally or unintentionally misguide readers and towards suboptimal technology choices.

Benchmarks should also pave the way for revolutionary technologies to gain adoption.  When a new query engine figures out how to process data in a faster, more reliable manner, they should be able to quantify improvements to users via benchmarks.  This helps drive adoption.
