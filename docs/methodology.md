# QueryBench Methodology

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

TODO

## Dataset sizes

TODO

## File formats

TODO

## Lazy vs. eager computations

TODO

## Single node vs. distributed compute engines

TODO

## Using the same methodology for all engines

TODO

## OLAP vs. OLTP engines

An engine that's optimized for OLAP queries typically cannot perform OLTP queries as quickly.  A set of queries that simply shows an OLAP engine can perform OLAP queries faster then a OLTP engine isn't particularily interesting.  It's better to show how one engine can perform the OLAP queries faster, but the other excels at OLTP workflows.
