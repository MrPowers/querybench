# QueryBench

QueryBench provides high-quality benchmarks for popular engines, so you can easily identify the best tool for your workloads.

For example, here's an example of some groupby query runtimes on a 100 million row dataset with various engines:

![](./images/groupby-fast-1e8.png)

Here's another set of benchmarks for a different dataset and set of queries:

![](./images/clickbench-slow.svg)

This is a comparison of great engines and the results are mixed!  You need to dig into the details a bit when you're selecting the best engine for your workflows.  Don't rely on overly simplistic studies that reduce benchmarks too a single graph - that's usually too simplistic for an intricate decision.

## QueryBench project goals

* Help users select fast engines for their workflows
* Help query engine maintainers identify areas of improvement
* Provide best practices for the data industry for building excellent benchmarks

## QueryBench methodology

QueryBench provides many types of benchmarks to account for tradeoffs with different engines.

Here are some guiding principles:

1. show the good and bad side of each engine
1. use entirely different query types to show edge case performance (e.g. show group by aggregations on a single low cardinarly column and on many high cardinality columns)
1. build benchmarks that are easy to reproduce
2. benchmark total runtime, including time it takes to load into memory
3. don't allow a single query to bias the overall results

## The data industry doesn't trust benchmarks

The data community is tired of VendorX publishing biased benchmarks that shows that their QueryEngineX is the best for all workflows.

You've probably seen the common errors:

* a study that tunes the queries for QueryEngineX and doesn't tune the other engines
* carefully selected queries that work especially well for a certain engine
* hardware that works best for their query engine
* using engines far from their designed purpose, like using an engine that's built to be distributed amongst many nodes in a cluster on a single node
* using a file format such that filesystem I/O takes more time than actually executing the query

## Why benchmarks are important

Analyzing modern query engines and choosing the right tool for the job takes a lot of time and effort.  It's easier to determine the best options based on reliable benchmarks, determine the engine with functionality that matches your workflows, and start by trying out the best performing engine.

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

Benchmarks can be harmful when they're biased or improperly structured and give misleading conclusions.  Benchmarks should not intentionally or unintentionally misguide readers and towards suboptimal technology choices.

Benchmarks should also pave the way for revolutionary technologies to gain adoption.  When a new query engine figures out how to process data in a faster, more reliable manner, they should be able to quantify improvements to users via benchmarks.  This helps drive adoption.

## The best query engine depends on many factors

Choosing the best engine is multifaceted, so the performance of the engine for certain queries isn't the only decision factor.

Here are other important factors to consider:

* OLTP vs. OLAP query patterns
* ad hoc vs. consistent querying
* skill set and preferences of your team

QueryBench aims to provides many types of benchmarks so you can identify the fastest engine for your team.  It strives to highlight potential biases for the different query patterns rather than hide these important details.

## Query engine speed isn't the only factor

Make sure to put these results in perspecive and remember that query engine speed is not the only important factor when selecting a query engine.

Here are some other important factors:

* library ecosystem: you may want to use a specific engine that's compatible with an important library you need for an analysis
* team skill set: your team may only know Python, so a Java engine isn't a good option for them
* team preferences: your team may be passionate about the tidyverse and R programming language, so it may not be worth switching to a different ecosystem
* data set size: your data may be large, so you have to use a big data engine.  Or, your data may be small, so there is no reason to take on the operational complexity of running a distributed engine.

If you follow the Lakehouse architecture, you can use many engines.

## You can use many engines


