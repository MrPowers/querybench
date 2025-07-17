import polars as pl
import daft

import sys
import pandas as pd
import querybench.datafusion.datafusion_clickbench_queries as datafusion_clickbench_queries
import querybench.daft.daft_clickbench_queries as daft_clickbench_queries
import querybench.polars.polars_clickbench_queries as polars_clickbench_queries
import querybench.duckdb.duckdb_clickbench_queries as duckdb_clickbench_queries
from datafusion import SessionContext
import os

path = sys.argv[1]

# polars
print("*** Polars ***")
df = pl.scan_parquet(path, low_memory=True)
df = df.with_columns(
    (pl.col("EventTime") * int(1e6)).cast(pl.Datetime(time_unit="us")),
    pl.col("EventDate").cast(pl.Date),
)
polars_res = polars_clickbench_queries.run_benchmarks(df).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
print("***")
print(path)
df = ctx.register_parquet("hits", path)
datafusion_res = datafusion_clickbench_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
print("*** Daft ***")
df = daft.read_parquet(path)
daft_res = daft_clickbench_queries.run_benchmarks(df).rename(columns={"duration": "daft"})

# duckdb
print("*** DuckDB ***")
duckdb_res = duckdb_clickbench_queries.run_benchmarks(path).rename(columns={"duration": "duckdb"})

# all results
res = (
    datafusion_res
    .join(duckdb_res, on="task")
    .join(polars_res, on="task")
    .join(daft_res, on="task")
)
print(res)

# res = duckdb_res
# print(res)

very_fast_queries = ['q0', 'q1', 'q2', 'q3', 'q6', 'q7', 'q10', 'q11', 'q19', 'q36', 'q37', 'q38', 'q39', 'q40', 'q41', 'q42']
slow_queries = ['q18', 'q22', 'q23', 'q28', 'q32', 'q33', 'q34']

# DataFrame with rows where 'task' is in the list
df_slow = res[res.index.isin(slow_queries)]

# DataFrame with the remaining rows
df_fast = res[~res.index.isin(slow_queries + very_fast_queries)]

df_very_fast = res[res.index.isin(very_fast_queries)]

# make slow plot
ax = df_slow.plot.bar(rot=0)
ax.set_title(f'ClickBench Slow Queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.figure.savefig(f"images/clickbench-slow.svg")
ax.figure.savefig(f"docs/images/clickbench-slow.svg")

# make fast plot
ax = df_fast.plot.bar(rot=0)
ax.set_title(f'ClickBench Fast Queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.tick_params(axis='x', labelsize=5)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
ax.figure.savefig(f"images/clickbench-fast.svg")
ax.figure.savefig(f"docs/images/clickbench-fast.svg")

# make very fast plot
ax = df_very_fast.plot.bar(rot=0)
ax.set_title(f'ClickBench Very Fast Queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.tick_params(axis='x', labelsize=5)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
ax.figure.savefig(f"images/clickbench-very-fast.svg")
ax.figure.savefig(f"docs/images/clickbench-very-fast.svg")