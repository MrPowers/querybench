# QueryBench Methodology

QueryBench uses benchmarking methdology that's fair for all engines.

The queries are designed to highlight the strenghts and weaknesses of engines.

The methodology is designed to level-out obvious biases found in other benchmarks.  Let's start by looking at some of the queries.

## Query selection



## In memory queries vs. cold queries



## Dataset sizes



## File formats



## Lazy vs. eager computations



## Single node vs. distributed compute engines



## Using the same methodology for all engines


## OLAP vs. OLTP

An engine that's optimized for OLAP queries typically cannot perform OLTP queries as quickly.  A set of queries that simply shows an OLAP engine can perform OLAP queries faster then a OLTP engine isn't particularily interesting.  It's better to show how one engine can perform the OLAP queries faster, but the other excels at OLTP workflows.
