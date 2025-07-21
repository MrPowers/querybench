# Contributing to QueryBench

## Generating datasets

Use [falsa](https://github.com/mrpowers-io/falsa) to generate the dataset.

You can use this command: `falsa groupby --path-prefix=~/data --size SMALL --data-format PARQUET`.

Here's how to run the 1e7 join h2o benchmarks: `uv run -m querybench.run_join 1e7 `.

Here are the different scale factors:

* Small dataset has 10 million rows (1e7) and runs quite fast
* Medium dataset has 100 million rows (1e8) and runs slower
* Large dataset has 1 billion rows (1e9) and often causes memory errors - it's a good way to stress test query engines on a single machine.

## Set environment variables

It's easier to run the benchmarks if you set the appropriate environment variables to the datasets:

Here are the environment variables to set (update the file paths depending on where the files are located on your machine):

```
export G1_1e7_1e2=/Users/matthewpowers/data/G1_1e7_1e2_0_0.parquet
export G1_1e8_1e2=/Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
export J1_1e7_1e1=/Users/matthewpowers/data/J1_1e7_1e1_0_0.parquet
export J1_1e7_1e4=/Users/matthewpowers/data/J1_1e7_1e4_0_0.parquet
export J1_1e7_1e7=/Users/matthewpowers/data/J1_1e7_1e7_0_0.parquet
export J1_1e7_NA=/Users/matthewpowers/data/J1_1e7_NA_0_0.parquet
export J1_1e8_1e2=/Users/matthewpowers/data/J1_1e8_1e2_0_0.parquet
export J1_1e8_1e5=/Users/matthewpowers/data/J1_1e8_1e5_0_0.parquet
export J1_1e8_1e8=/Users/matthewpowers/data/J1_1e8_1e8_0_0.parquet
export J1_1e8_NA=/Users/matthewpowers/data/J1_1e8_NA_0_0.parquet
export clickbench=/Users/matthewpowers/data/hits.parquet
```

## Run scripts

You can run all the scripts as follows:

```sh
scripts/run_all.sh
```

This query takes 40 minutes to run.

You can run the individual scrips as follows:

```
uv run -m querybench.run_groupby 1e8
```

```
uv run -m querybench.run_join 1e7
```

```
uv run -m querybench.run_single_table 1e7
```

```
uv run -m querybench.run_clickbench $clickbench
```

These scripts output benchmarks in `images/`.
