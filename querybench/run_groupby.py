import polars as pl
import daft

import sys
import pandas as pd
from querybench.polars import polars_h2o_groupby_queries
from querybench.datafusion import datafusion_h2o_groupby_queries
from querybench.daft import daft_h2o_groupby_queries
from querybench.duckdb import duckdb_h2o_groupby_queries
from datafusion import SessionContext
import os

path = sys.argv[1]
if path == "1e8" or path == "1e7":
    num_rows = sys.argv[1]
if path == "1e8":
    path = os.getenv("G1_1e8_1e2")
elif path == "1e7":
    path = os.getenv("G1_1e7_1e2")

# polars
print("*** Polars ***")
df = pl.scan_parquet(path, low_memory=True)
polars_res = polars_h2o_groupby_queries.run_benchmarks([df]).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
df = ctx.register_parquet("x", path)
datafusion_res = datafusion_h2o_groupby_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
print("*** Daft ***")
df = daft.read_parquet(path)
daft_res = daft_h2o_groupby_queries.run_benchmarks([df]).rename(columns={"duration": "daft"})

# duckdb
print("*** DuckDB ***")
duckdb_res = duckdb_h2o_groupby_queries.run_benchmarks([path]).rename(columns={"duration": "duckdb"})

# all results
res = (
    datafusion_res
    .join(duckdb_res, on="task")
    .join(polars_res, on="task")
    .join(daft_res, on="task")
)
print(res)

slow_queries = ['q10']

# DataFrame with rows where 'task' is in the list
df_slow = res[res.index.isin(slow_queries)]

# DataFrame with the remaining rows
df_fast = res[~res.index.isin(slow_queries)]

if num_rows:
    ax = df_fast.plot.bar(rot=0)
    ax.set_title(f'Fast h2o groupby queries ({num_rows})')
    ax.set_ylabel('Seconds')
    ax.set_xlabel('Queries')
    ax.figure.savefig(f"images/groupby-fast-{num_rows}.svg")
    ax.figure.savefig(f"docs/images/groupby-fast-{num_rows}.svg")

if num_rows:
    ax = df_slow.plot.bar(rot=0)
    ax.set_title(f'Fast h2o groupby queries ({num_rows})')
    ax.set_ylabel('Seconds')
    ax.set_xlabel('Queries')
    ax.figure.savefig(f"images/groupby-slow-{num_rows}.svg")
    ax.figure.savefig(f"docs/images/groupby-slow-{num_rows}.svg")
