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
