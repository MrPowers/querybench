# Queries

This page covers the logic behind the various queries in QueryBench.

## QueryBench

### Single table

TODO: Add some single table queries on the Clickhouse table

## h2o

### h2o join

**Note: This section assums the 1e8 dataset size**

The first h2o join query runs a join operation on a large table and a small table:

```sql
SELECT x.id1, x.id2, x.id3, x.id4 as xid4, small.id4 as smallid4, x.id5, x.id6, x.v1, small.v2
FROM x
INNER JOIN small ON x.id1 = small.id1
```

This query returns XX rows.

### h2o groupby

**Note: This section assums the 1e8 dataset size**

The first h2o groupby query runs a group by aggregation on a low cardinality column:

```sql
select id1, sum(v1) as v1
from x
group by id1
```

The `id1` column has 100 distinct values, so this query returns 100 results.  Here's what the result looks like:

XXX

The second h2o group by query runs a group by aggregation on two columns:

```sql
select id1, id2, sum(v1) as v1 
from x
group by id1, id2
```

Each id1 value has 100 distinct id2 values, so this query returns 10,000 results.  Here's what they look like:

XXX

TODO

## Clickbench

TODO

## TPC-H

Here are the results of the TPC-H queries on scale factor 50.

**query 1**

```sql
select
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
  from
      read_parquet('lineitem.parquet')
  where
      l_shipdate <= date '1998-12-01' - interval '68 days'
  group by
      l_returnflag,
      l_linestatus
  order by
      l_returnflag,
      l_linestatus;
```

```
┌──────────────┬──────────────┬───────────────┬──────────────────┬───┬────────────────────┬─────────────────────┬─────────────┐
│ l_returnflag │ l_linestatus │    sum_qty    │  sum_base_price  │ … │     avg_price      │      avg_disc       │ count_order │
│   varchar    │   varchar    │ decimal(38,2) │  decimal(38,2)   │   │       double       │       double        │    int64    │
├──────────────┼──────────────┼───────────────┼──────────────────┼───┼────────────────────┼─────────────────────┼─────────────┤
│ A            │ F            │ 1887655913.00 │ 2830563886920.51 │ … │  38236.55666404859 │ 0.05000122818910676 │    74027688 │
│ N            │ F            │   49261643.00 │   73891810316.90 │ … │ 38265.510417968864 │ 0.04997881440413376 │     1931029 │
│ N            │ O            │ 3763646874.00 │ 5643468843164.77 │ … │  38234.30610204717 │  0.0499975012572642 │   147602230 │
│ R            │ F            │ 1887847853.00 │ 2830705147172.23 │ … │  38239.13019101604 │  0.0499987842175224 │    74026400 │
├──────────────┴──────────────┴───────────────┴──────────────────┴───┴────────────────────┴─────────────────────┴─────────────┤
│ 4 rows                                                                                                 10 columns (7 shown) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**query 2**

```sql
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	read_parquet('part.parquet'),
	read_parquet('supplier.parquet'),
	read_parquet('partsupp.parquet'),
	read_parquet('nation.parquet'),
	read_parquet('region.parquet')
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 48
	and p_type like '%TIN'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			read_parquet('partsupp.parquet'),
			read_parquet('supplier.parquet'),
			read_parquet('nation.parquet'),
			read_parquet('region.parquet')
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'ASIA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey limit 100;
```

```
┌───────────────┬────────────────────┬───────────┬───────────┬───┬──────────────────────┬─────────────────┬──────────────────────┐
│   s_acctbal   │       s_name       │  n_name   │ p_partkey │ … │      s_address       │     s_phone     │      s_comment       │
│ decimal(15,2) │      varchar       │  varchar  │   int64   │   │       varchar        │     varchar     │       varchar        │
├───────────────┼────────────────────┼───────────┼───────────┼───┼──────────────────────┼─────────────────┼──────────────────────┤
│       9999.37 │ Supplier#000152165 │ JAPAN     │   5527153 │ … │ O3zuZZ2V88TMt3       │ 22-464-260-2745 │ press packages boo…  │
│       9999.18 │ Supplier#000187179 │ INDONESIA │   1562175 │ … │ cuoImj9sQC1GcN2JiL…  │ 19-342-626-8923 │ al deposits. expre…  │
│       9997.92 │ Supplier#000152897 │ INDONESIA │   2152896 │ … │ VS8DqGjgNyh5toWUEh…  │ 19-966-614-3374 │ al excuses. pendin…  │
│       9997.66 │ Supplier#000448507 │ JAPAN     │    823505 │ … │ aLh8mUTUIn6J9YnwnB…  │ 22-923-823-3068 │ requests haggle ac…  │
│       9996.50 │ Supplier#000313254 │ INDIA     │   7563223 │ … │ 6RNfzGBqM8           │ 18-303-327-8242 │ ickly accounts. ac…  │
│       9995.92 │ Supplier#000186115 │ CHINA     │   4061106 │ … │ pHMjbQEXuRN9caU9oq…  │ 28-791-179-8755 │ lyly quickly expre…  │
│       9995.57 │ Supplier#000455805 │ VIETNAM   │   1330802 │ … │  Wk0vgC Te,QYmF07GaQ │ 31-544-665-8405 │ oxes boost blithel…  │
│       9995.46 │ Supplier#000375089 │ JAPAN     │    625086 │ … │ IOZV9bXS3wcGpnpL9p…  │ 22-395-477-2710 │ ronic requests. ca…  │
│       9995.46 │ Supplier#000375089 │ JAPAN     │   8500037 │ … │ IOZV9bXS3wcGpnpL9p…  │ 22-395-477-2710 │ ronic requests. ca…  │
│       9994.86 │ Supplier#000089719 │ INDONESIA │     89718 │ … │ lDNpZesVZBeI7Aj6pL…  │ 19-995-912-8747 │ unusual deposits. …  │
│       9993.65 │ Supplier#000366299 │ INDIA     │   2866298 │ … │ tjIr5eEbmK4RNbFHCi…  │ 18-756-609-2929 │ mong the quickly i…  │
│       9993.60 │ Supplier#000169267 │ CHINA     │   2919256 │ … │ mF2CJKjYGeuR7MUwOL…  │ 28-862-242-5018 │ atelets. slyly unu…  │
│       9993.43 │ Supplier#000143639 │ CHINA     │   9268584 │ … │ pNes0R0hKQ cNsf2rY1  │ 28-568-610-3826 │ o beans sublate bl…  │
│       9993.02 │ Supplier#000316864 │ JAPAN     │   5316863 │ … │ YYJr8bHdbG,haTd gS…  │ 22-793-479-9247 │ uickly final excus…  │
│       9993.02 │ Supplier#000316864 │ JAPAN     │   7691848 │ … │ YYJr8bHdbG,haTd gS…  │ 22-793-479-9247 │ uickly final excus…  │
│       9992.55 │ Supplier#000389874 │ VIETNAM   │   4639855 │ … │ gUzhYbjfEe95LLKbtl…  │ 31-265-401-2943 │ kages integrate. a…  │
│       9992.55 │ Supplier#000389874 │ VIETNAM   │   9639835 │ … │ gUzhYbjfEe95LLKbtl…  │ 31-265-401-2943 │ kages integrate. a…  │
│       9991.96 │ Supplier#000375427 │ VIETNAM   │   4250418 │ … │ bjM9DvUuMtoN6EIGjk…  │ 31-517-551-2958 │ fully; blithely ir…  │
│       9991.53 │ Supplier#000100632 │ JAPAN     │   1225625 │ … │ 1ONN0E6KUqAEQ0LtfS8H │ 22-626-411-4815 │ sleep pending pack…  │
│       9991.32 │ Supplier#000084065 │ VIETNAM   │   5709031 │ … │ CfJ,Bun49DmmJ        │ 31-355-724-3538 │ ross the slyly reg…  │
│          ·    │         ·          │   ·       │      ·    │ · │       ·              │        ·        │          ·           │
│          ·    │         ·          │   ·       │      ·    │ · │       ·              │        ·        │          ·           │
│          ·    │         ·          │   ·       │      ·    │ · │       ·              │        ·        │          ·           │
│       9961.58 │ Supplier#000030247 │ INDIA     │   2405242 │ … │ NCBeIFhm0msmQpQlYm…  │ 18-294-166-5475 │  even theodolites …  │
│       9961.58 │ Supplier#000030247 │ INDIA     │   5155216 │ … │ NCBeIFhm0msmQpQlYm…  │ 18-294-166-5475 │  even theodolites …  │
│       9961.14 │ Supplier#000285024 │ JAPAN     │   8660006 │ … │ mGHXOp2DT8SZ7        │ 22-752-579-9074 │ carefully furiousl…  │
│       9961.12 │ Supplier#000445320 │ VIETNAM   │   3195307 │ … │ QESA9oF5VSJLT SLJ1…  │ 31-236-344-3186 │ ions sleep across …  │
│       9961.06 │ Supplier#000314108 │ INDONESIA │   6564081 │ … │ l6B,v9O6JQ6DtWuPWa…  │ 19-280-239-1224 │ o the furiously ev…  │
│       9960.97 │ Supplier#000299019 │ INDIA     │   9423964 │ … │ yOdzQignZNgcPcn3FWt  │ 18-708-979-6992 │ . unusual, ironic …  │
│       9960.24 │ Supplier#000230684 │ JAPAN     │    230683 │ … │ ObbCxsX3FRB6NMv4zw…  │ 22-722-401-5029 │ ns boost carefully…  │
│       9959.74 │ Supplier#000263668 │ INDONESIA │   5013647 │ … │ 2bfYZNPnr3dVzd9yW7…  │ 19-874-159-9792 │  carefully above t…  │
│       9959.44 │ Supplier#000405159 │ JAPAN     │   5780147 │ … │ NMv4Gfj7QEnn9gW,bj…  │ 22-736-487-5967 │ lithely regular ac…  │
│       9959.15 │ Supplier#000255886 │ INDONESIA │   9255885 │ … │ AaLtkfxDjhmaJQVQcu…  │ 19-703-242-2420 │  pending instructi…  │
│       9959.04 │ Supplier#000141451 │ INDONESIA │   1141450 │ … │ bS98aTSOlzlolEPxrC…  │ 19-103-563-6923 │ ar packages nag ca…  │
│       9957.81 │ Supplier#000367491 │ VIETNAM   │   9742471 │ … │ d 4vQnXZCN           │ 31-777-540-6844 │ s. silently regula…  │
│       9956.55 │ Supplier#000000693 │ INDIA     │   7875677 │ … │ S,mnHfsroFOVieQGdc…  │ 18-231-996-9225 │ wake quickly aroun…  │
│       9956.30 │ Supplier#000413826 │ CHINA     │   3038807 │ … │ 2GYCG8IHPbyxV7XusU…  │ 28-934-282-6512 │ inal deposits hagg…  │
│       9955.96 │ Supplier#000396174 │ INDIA     │   9396173 │ … │ rPJZVSg6UKbSNrW3tP   │ 18-744-572-3905 │ nal requests are a…  │
│       9955.93 │ Supplier#000212273 │ JAPAN     │   7462244 │ … │ xYEmKF,mABtVAUsGdo…  │ 22-153-555-5676 │ ideas against the …  │
│       9955.64 │ Supplier#000333502 │ INDIA     │   5458471 │ … │ Jvb8iD7O2yDokZcp4b…  │ 18-114-488-5444 │  furiously even so…  │
│       9955.64 │ Supplier#000333502 │ INDIA     │   8833501 │ … │ Jvb8iD7O2yDokZcp4b…  │ 18-114-488-5444 │  furiously even so…  │
│       9955.61 │ Supplier#000223142 │ INDIA     │   9723141 │ … │ 86qtwbuYKatIXST4c4…  │ 18-688-680-3959 │ ong the accounts; …  │
│       9954.64 │ Supplier#000027733 │ JAPAN     │   1027732 │ … │ wMLQPkLkripOnC5tNR…  │ 22-787-697-5085 │ bold accounts. spe…  │
├───────────────┴────────────────────┴───────────┴───────────┴───┴──────────────────────┴─────────────────┴──────────────────────┤
│ 100 rows (40 shown)                                                                                        8 columns (7 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**query 3**

```sql
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	read_parquet('customer.parquet'),
	read_parquet('orders.parquet'),
	read_parquet('lineitem.parquet')
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate limit 10;
```

```
┌────────────┬───────────────┬─────────────┬────────────────┐
│ l_orderkey │    revenue    │ o_orderdate │ o_shippriority │
│   int64    │ decimal(38,4) │    date     │     int32      │
├────────────┼───────────────┼─────────────┼────────────────┤
│   73692448 │   471406.9264 │ 1995-03-10  │              0 │
│   45428128 │   465008.1878 │ 1995-02-09  │              0 │
│   10725381 │   463474.5187 │ 1995-03-12  │              0 │
│   51720326 │   462037.9005 │ 1995-03-13  │              0 │
│   31692004 │   460816.8551 │ 1995-03-10  │              0 │
│   99055008 │   456576.5414 │ 1995-03-07  │              0 │
│  223437735 │   451524.3011 │ 1995-03-06  │              0 │
│  142926083 │   450314.6518 │ 1995-02-21  │              0 │
│  187603874 │   449114.1737 │ 1995-02-23  │              0 │
│  149995427 │   448828.4196 │ 1995-03-02  │              0 │
├────────────┴───────────────┴─────────────┴────────────────┤
│ 10 rows                                         4 columns │
└───────────────────────────────────────────────────────────┘
```

**query 4**

```sql
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	read_parquet('customer.parquet'),
	read_parquet('orders.parquet'),
	read_parquet('lineitem.parquet'),
	read_parquet('supplier.parquet'),
	read_parquet('nation.parquet'),
	read_parquet('region.parquet')
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AFRICA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1994-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc;
```

```
┌────────────┬─────────────────┐
│   n_name   │     revenue     │
│  varchar   │  decimal(38,4)  │
├────────────┼─────────────────┤
│ ETHIOPIA   │ 2670237074.6808 │
│ MOZAMBIQUE │ 2638671522.5894 │
│ MOROCCO    │ 2630639127.4946 │
│ ALGERIA    │ 2603745850.4060 │
│ KENYA      │ 2602939797.5912 │
└────────────┴─────────────────┘
```
