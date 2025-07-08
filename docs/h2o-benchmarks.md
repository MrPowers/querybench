# h2o benchmarks

The h2o benchmarks have certain limitations, as do all benchmarks.

This section explains some of the limitations of the h2o benchmarks, not as a criticism, but to explain the tradeoffs that were made in the h2o benchmarks.  The h2o benchmarks are an excellent contribution to the data community and we should be grateful for the engineers that dedicated their time and effort to make them available.

### Single CSV file

The h2o benchmarks are run on data that's stored in a single CSV file.

This introduces a bias for query engines that are optimzied for running on a single file.  This bias is somewhat offset because the data is persisted in memory before the queries are run.

Remember that CSV is a row based file format and column projection is not supported.  You can't cherry pick certain columns and persist them in memory when running queries.

CSVs also require for schemas to be inferred or manually specified.  If one engine manually specifies an efficient type (e.g. int32) and another engine infers a less efficient type (e.g. int64) for a given column, then the query comparisons are biased.  Having all engines use the same types defined in the Parquet file provides a more even comparison.

### Data persisted in memory

The h2o benchmarks persist data in memory, but they are using CSV files, so they need to persist all the data in memory at once.  They can't just persist the columns that are relevant to the query.  Persisting all the columns causes certain queries to error out that wouldn't otherwise have issues if only 2 or 3 columns were persisted.

Persisting data in memory also hides performance benefits of querie engines that are capable of performing parallel I/O.

### Presenting overall query runtime is misleading

The h2o benchmarks show the runtimes for each individual query and all the queries summed.  The sum amount can be greatly impacted by one slowly running query, so it's potentially misleading.

### Engines that support parallel I/O

Certain query engines are designed to read and write data in parallel - others are not.

Query engine users often care about the entire time it takes to run a query.  How long it takes to read the data, persist it in memory, and execute the query.  The h2o benchmarks only give readers information on how long it takes to actually execute the query.  This is certainly valuable information, but potentially misleading for the 50 GB benchmarks.  Reading 50 GB of data is a significant performance advantage compared to reading a single file.

### Small datasets

The h2o benchmarks only test 0.5 GB, 5 GB, and 50 GB datasets.  They don't test 500 GB or 5 TB datasets.

This introduces a bias for query engines that can run on small datasets, but can't work on larger datsets.

### Distributed engines vs single machine engines

Distributed query engines usually make tradeoffs so they're optimized to be run on a cluster of computers.  The h2o benchmarks are run on a single machine, so they're biased against distributed query engines.

Distributed query engines of course offer a massive benefit for data practitioners.  They can be scaled beyond the limits of a single machine and can be used to run analysis on large datasets.  Including benchmarks on larger datasets with distributed clusters is a good way to highlight the strenghts of query engines that are designed to scale.  This is also a good way to highlight the data volume limits of query engines designed to run on a single machine.

### GPUs vs CPUs

The h2o bencharks include query engines that are designed to be run on CPUs and others that are designed to be run on GPUs.  There are a variety of ways to present benchmarking results from different hardware:

* present the results side-by-side, in the same graph
* present the results in different graphs
* present cost adjusted results

h2o decided to present the results side-by-side, which is reasonable, but there are other ways these results could habe been presented too.

### Query optimization

The h2o benchmarks are well specified, so they don't give engines that have query optimizers a chance to shine.  Engines with a query optimizer will rearrange a poorly specified query and make it run better.  The h2o benchmarks could have included some poorly specified queries to highlight they query optimization strenghts of certain engines.

### Lazy computations and collecting results

Certain query engines support lazy execution, so they don't actually run computations until it's absolutely necessary.  Other query engines eagerly execute computations whenever a command is run.

Lazy computation engines generally split up data and execute computations in parallel.  They don't collect results into a single DataFrame by default because it's usually better to keep the data split up for additional parallelism for downstream computations.

Running a query for a lazy computation engine generally involves two steps:

* actually running the computation
* collecting the subresults into a single DataFrame

Collecting the results into a single DataFrame arguably should not be included in the parallel engine computation runtime.  That's an extra step that's required to get the result, but not usually necessary in a real-world use case.

It can unfortunately be hard to divide a query runtime into different compontents.  Most parallel compute engine query runtimes include both, which is probably misleading.