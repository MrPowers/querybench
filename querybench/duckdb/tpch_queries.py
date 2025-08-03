from querybench.helpers import benchmark, get_results
import duckdb
import re
import querybench


def replace_sql_with_parquet_paths(sql: str, paths) -> str:
    customer, lineitem, nation, orders, part, partsupp, region, supplier = paths
    sql = re.sub(r'\bcustomer\b', f"read_parquet('{customer}')", sql)
    sql = re.sub(r'\blineitem\b', f"read_parquet('{lineitem}')", sql)
    sql = re.sub(r'\bnation\b', f"read_parquet('{nation}')", sql)
    sql = re.sub(r'\borders\b', f"read_parquet('{orders}')", sql)
    sql = re.sub(r'\bpart\b', f"read_parquet('{part}')", sql)
    sql = re.sub(r'\bpartsupp\b', f"read_parquet('{partsupp}')", sql)
    sql = re.sub(r'\bregion\b', f"read_parquet('{region}')", sql)
    sql = re.sub(r'\bsupplier\b', f"read_parquet('{supplier}')", sql)
    return sql


def q1(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q1(), paths)
    return duckdb.sql(sql).df()


def q2(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q2(), paths)
    print(sql)
    return duckdb.sql(sql).df()


def q3(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q3(), paths)
    print(sql)
    return duckdb.sql(sql).df()


def q4(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q4(), paths)
    print(sql)
    return duckdb.sql(sql).df()


def q5(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q5(), paths)
    print(sql)
    return duckdb.sql(sql).df()


def q6(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q6(), paths)
    return duckdb.sql(sql).df()


def q7(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q7(), paths)
    return duckdb.sql(sql).df()


def q8(paths):
    customer, lineitem, nation, orders, part, partsupp, region, supplier = paths
    sql = f"""
select
	o_year,
	sum(case
		when nation = 'IRAQ' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			read_parquet('{part}'),
			read_parquet('{supplier}'),
			read_parquet('{lineitem}'),
			read_parquet('{orders}'),
			read_parquet('{customer}'),
			read_parquet('{nation}') n1,
			read_parquet('{nation}') n2,
			read_parquet('{region}')
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'MIDDLE EAST'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'LARGE PLATED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;
    """
    return duckdb.sql(sql).df()


def q9(paths):
    customer, lineitem, nation, orders, part, partsupp, region, supplier = paths
    sql = f"""
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			read_parquet('{part}'),
			read_parquet('{supplier}'),
			read_parquet('{lineitem}'),
			read_parquet('{partsupp}'),
			read_parquet('{orders}'),
			read_parquet('{nation}')
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%moccasin%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
    """
    return duckdb.sql(sql).df()


def q10(paths):
    customer, lineitem, nation, orders, part, partsupp, region, supplier = paths
    sql = f"""
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	read_parquet('{customer}'),
	read_parquet('{orders}'),
	read_parquet('{lineitem}'),
	read_parquet('{nation}')
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc limit 20;
    """
    return duckdb.sql(sql).df()


def q11(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q11(), paths)
    return duckdb.sql(sql).df()


def q12(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q12(), paths)
    return duckdb.sql(sql).df()



def q13(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q13(), paths)
    return duckdb.sql(sql).df()


def q14(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q14(), paths)
    return duckdb.sql(sql).df()


def q15(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q15(), paths)
    print("%%##")
    print(sql)
    return duckdb.sql(sql).df()


def q16(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q16(), paths)
    return duckdb.sql(sql).df()


def q16(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q16(), paths)
    return duckdb.sql(sql).df()


def q17(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q17(), paths)
    return duckdb.sql(sql).df()

def q18(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q18(), paths)
    return duckdb.sql(sql).df()


def q19(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q19(), paths)
    return duckdb.sql(sql).df()

def q20(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q20(), paths)
    return duckdb.sql(sql).df()


def q21(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q21(), paths)
    return duckdb.sql(sql).df()


def q22(paths):
    sql = replace_sql_with_parquet_paths(querybench.queries.tpch.q22(), paths)
    return duckdb.sql(sql).df()


def run_benchmarks(paths):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, paths, benchmarks=benchmarks, name="q1")
    benchmark(q2, paths, benchmarks=benchmarks, name="q2")
    benchmark(q3, paths, benchmarks=benchmarks, name="q3")
    benchmark(q4, paths, benchmarks=benchmarks, name="q4")
    benchmark(q5, paths, benchmarks=benchmarks, name="q5")
    benchmark(q6, paths, benchmarks=benchmarks, name="q6")
    benchmark(q7, paths, benchmarks=benchmarks, name="q7")
    benchmark(q8, paths, benchmarks=benchmarks, name="q8")
    benchmark(q9, paths, benchmarks=benchmarks, name="q9")
    benchmark(q10, paths, benchmarks=benchmarks, name="q10")
    benchmark(q11, paths, benchmarks=benchmarks, name="q11")
    benchmark(q12, paths, benchmarks=benchmarks, name="q12")
    benchmark(q13, paths, benchmarks=benchmarks, name="q13")
    benchmark(q14, paths, benchmarks=benchmarks, name="q14")
    # benchmark(q15, paths, benchmarks=benchmarks, name="q15")
    benchmark(q16, paths, benchmarks=benchmarks, name="q16")
    benchmark(q17, paths, benchmarks=benchmarks, name="q17")
    benchmark(q18, paths, benchmarks=benchmarks, name="q18")
    benchmark(q19, paths, benchmarks=benchmarks, name="q19")
    benchmark(q20, paths, benchmarks=benchmarks, name="q20")
    benchmark(q21, paths, benchmarks=benchmarks, name="q21")
    benchmark(q22, paths, benchmarks=benchmarks, name="q22")

    res = get_results(benchmarks).set_index("task")
    return res
