# Contributing to QueryBench

## Generating datasets

Use [falsa](https://github.com/mrpowers-io/falsa) to generate the dataset.

You can use this command: `falsa groupby --path-prefix=~/data --size SMALL --data-format PARQUET`.

Here's how to run the benchmarks in this project: `uv run benchmarks/run_all_groupby.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet`.

The small dataset has 10 million rows (1e7) and runs quite fast.  The medium dataset has 100 million rows (1e8) and runs slower.  The large dataset has 1 billion rows (1e9) and often causes memory errors - it's a good way to stress test query engines on a single machine.

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
```

## Run scripts

You can type in the full path to the dataset when running commands:

```
uv run benchmarks/run_all_groupby.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
```

Or you can supply an argument with the path to the file:

```
uv run benchmarks/run_all_groupby.py 1e8
```

```
uv run benchmarks/run_all_single_table.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
```

```
uv run benchmarks/datafusion_h2o_join.py /Users/matthewpowers/data/J1_1e7_NA_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e1_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e4_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e7_0_0.parquet
```

```
uv run benchmarks/datafusion_h2o_join.py /Users/matthewpowers/data/J1_1e8_NA_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e2_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e5_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e8_0_0.parquet
```

## Convert CSV to Parquet

Here is how you can convert a CSV file to Parquet: `csv_to_parquet(csv_path, parquet_path)`.

Here is how you can convert a directory of CSV files to Parquet files:

```
uv run benchmarks/convert_csvs.py ~/Documents/code/cloned/db-benchmark/_data ~/data
```

## Generating h2o CSV datasets with h2o R code

Here's how to generate the h2o datasets.

* Run `conda env create -f envs/mr-r.yml` to create an environment with R
* Activate the environment with `conda activate mr-r`
* Open a R session by typing `R` in your Terminal
* Install the required package with `install.packages("data.table")`
* Exit the R session with `quit()`
* Respond with `y` when asked `Save workspace image? [y/n/c]` (not sure if this is needed)
* Clone the [db-benchmark](https://github.com/h2oai/db-benchmark) repo
* `cd` into the `_data` directory and run commands like `Rscript groupby-datagen.R 1e7 1e2 0 0` to generate the data files

Move these CSV files to the `~/data` directory on your machine:

```
~/data/
  G1_1e7_1e2_0_0.csv
  G1_1e8_1e2_0_0.csv
  G1_1e9_1e2_0_0.csv
```

Now create the Parquet files.  Create a conda environment with Delta Lake, PySpark, and PyArrow, like [this one](https://github.com/delta-io/delta-examples/blob/master/envs/pyspark-340-delta-240.yml).

Here's the script to convert from CSV to Parquet:

```
def csv_to_parquet(csv_path, parquet_path):
    writer = None
    with pyarrow.csv.open_csv(in_path) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pq.ParquetWriter(out_path, next_chunk.schema)
            next_table = pa.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()
```

Here's how you can create Delta tables from the Parquet files:

```
parquet_path = f"{Path.home()}/data/G1_1e9_1e2_0_0.parquet"
df = spark.read.format("parquet").load(parquet_path)
delta_path = f"{Path.home()}/data/deltalake/G1_1e9_1e2_0_0"
df.write.format("delta").save(delta_path)
```
