import polars as pl
import daft

import querybench
import sys
import pandas as pd
from datafusion import SessionContext
import duckdb
import os

path = sys.argv[1]
sf = sys.argv[2]

# # polars
# print("*** Polars ***")
# df = pl.scan_parquet(path, low_memory=True)
# df = df.with_columns(
#     (pl.col("EventTime") * int(1e6)).cast(pl.Datetime(time_unit="us")),
#     pl.col("EventDate").cast(pl.Date),
# )
# polars_res = polars_clickbench_queries.run_benchmarks(df).rename(columns={"duration": "polars"})
# print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
print("***")
print(path)
ctx.register_parquet("customer", f"{path}/tpch_sf{sf}/customer.parquet")
ctx.register_parquet("lineitem", f"{path}/tpch_sf{sf}/lineitem.parquet")
ctx.register_parquet("nation", f"{path}/tpch_sf{sf}/nation.parquet")
ctx.register_parquet("orders", f"{path}/tpch_sf{sf}/orders.parquet")
ctx.register_parquet("part", f"{path}/tpch_sf{sf}/part.parquet")
ctx.register_parquet("partsupp", f"{path}/tpch_sf{sf}/partsupp.parquet")
ctx.register_parquet("region", f"{path}/tpch_sf{sf}/region.parquet")
ctx.register_parquet("supplier", f"{path}/tpch_sf{sf}/supplier.parquet")
datafusion_res = querybench.datafusion.tpch_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# # daft
# print("*** Daft ***")
# df = daft.read_parquet(path)
# daft_res = daft_clickbench_queries.run_benchmarks(df).rename(columns={"duration": "daft"})

# duckdb
print("*** DuckDB ***")
paths = [
    f"{path}/tpch_sf{sf}/customer.parquet",
    f"{path}/tpch_sf{sf}/lineitem.parquet",
    f"{path}/tpch_sf{sf}/nation.parquet",
    f"{path}/tpch_sf{sf}/orders.parquet",
    f"{path}/tpch_sf{sf}/part.parquet",
    f"{path}/tpch_sf{sf}/partsupp.parquet",
    f"{path}/tpch_sf{sf}/region.parquet",
    f"{path}/tpch_sf{sf}/supplier.parquet"
]
duckdb_res = querybench.duckdb.tpch_queries.run_benchmarks(paths).rename(columns={"duration": "duckdb"})

# all results
res = (
    datafusion_res
    .join(duckdb_res, on="task")
    # .join(polars_res, on="task")
    # .join(daft_res, on="task")
)
# res = datafusion_res
print(res)
# res = datafusion_res

# res = duckdb_res
# print(res)

# very_fast_queries = ['q0', 'q1', 'q2', 'q3', 'q6', 'q7', 'q10', 'q11', 'q19', 'q36', 'q37', 'q38', 'q39', 'q40', 'q41', 'q42']
# slow_queries = ['q18', 'q22', 'q23', 'q28', 'q32', 'q33', 'q34']

# # DataFrame with rows where 'task' is in the list
# df_slow = res[res.index.isin(slow_queries)]

# # DataFrame with the remaining rows
# df_fast = res[~res.index.isin(slow_queries + very_fast_queries)]

# df_very_fast = res[res.index.isin(very_fast_queries)]

# make plot
ax = res.plot.bar(rot=0)
ax.set_title(f'TPC H Scale Factor {sf}')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
ax.figure.savefig(f"images/tpch-sf{sf}.svg")
ax.figure.savefig(f"docs/images/tpch-sf{sf}.svg")
