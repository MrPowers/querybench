# Datasets

## h20

### h2o groupby table

***
/Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
shape: (100_000_000, 9)
```
┌───────┬───────┬──────────────┬─────┬───┬────────┬─────┬─────┬───────────┐
│ id1   ┆ id2   ┆ id3          ┆ id4 ┆ … ┆ id6    ┆ v1  ┆ v2  ┆ v3        │
│ ---   ┆ ---   ┆ ---          ┆ --- ┆   ┆ ---    ┆ --- ┆ --- ┆ ---       │
│ str   ┆ str   ┆ str          ┆ i64 ┆   ┆ i64    ┆ i64 ┆ i64 ┆ f64       │
╞═══════╪═══════╪══════════════╪═════╪═══╪════════╪═════╪═════╪═══════════╡
│ id016 ┆ id046 ┆ id0000109363 ┆ 88  ┆ … ┆ 146094 ┆ 4   ┆ 6   ┆ 18.837686 │
│ id039 ┆ id087 ┆ id0000466766 ┆ 14  ┆ … ┆ 111330 ┆ 4   ┆ 14  ┆ 46.797328 │
│ id047 ┆ id098 ┆ id0000307804 ┆ 85  ┆ … ┆ 187639 ┆ 3   ┆ 5   ┆ 47.577311 │
│ id043 ┆ id017 ┆ id0000344864 ┆ 87  ┆ … ┆ 256509 ┆ 2   ┆ 5   ┆ 80.462924 │
│ id054 ┆ id027 ┆ id0000433679 ┆ 99  ┆ … ┆ 32736  ┆ 1   ┆ 7   ┆ 15.796662 │
│ …     ┆ …     ┆ …            ┆ …   ┆ … ┆ …      ┆ …   ┆ …   ┆ …         │
│ id080 ┆ id025 ┆ id0000598386 ┆ 43  ┆ … ┆ 56728  ┆ 3   ┆ 9   ┆ 27.47907  │
│ id064 ┆ id012 ┆ id0000844471 ┆ 19  ┆ … ┆ 203895 ┆ 4   ┆ 5   ┆ 5.323666  │
│ id046 ┆ id053 ┆ id0000544024 ┆ 31  ┆ … ┆ 711000 ┆ 5   ┆ 3   ┆ 27.827385 │
│ id081 ┆ id090 ┆ id0000802094 ┆ 53  ┆ … ┆ 57466  ┆ 1   ┆ 15  ┆ 23.319917 │
│ id001 ┆ id057 ┆ id0000141978 ┆ 93  ┆ … ┆ 224681 ┆ 4   ┆ 10  ┆ 91.788497 │
└───────┴───────┴──────────────┴─────┴───┴────────┴─────┴─────┴───────────┘
```
Schema({'id1': String, 'id2': String, 'id3': String, 'id4': Int64, 'id5': Int64, 'id6': Int64, 'v1': Int64, 'v2': Int64, 'v3': Float64})

***
/Users/matthewpowers/data/J1_1e8_NA_0_0.parquet
shape: (100_000_000, 7)
```
┌─────┬────────┬───────────┬───────┬──────────┬─────────────┬───────────┐
│ id1 ┆ id2    ┆ id3       ┆ id4   ┆ id5      ┆ id6         ┆ v1        │
│ --- ┆ ---    ┆ ---       ┆ ---   ┆ ---      ┆ ---         ┆ ---       │
│ i64 ┆ i64    ┆ i64       ┆ str   ┆ str      ┆ str         ┆ f64       │
╞═════╪════════╪═══════════╪═══════╪══════════╪═════════════╪═══════════╡
│ 32  ┆ 57316  ┆ 104012378 ┆ id32  ┆ id57316  ┆ id104012378 ┆ 2.184703  │
│ 17  ┆ 32099  ┆ 103369135 ┆ id17  ┆ id32099  ┆ id103369135 ┆ 26.295686 │
│ 106 ┆ 102270 ┆ 66344514  ┆ id106 ┆ id102270 ┆ id66344514  ┆ 34.744782 │
│ 99  ┆ 51861  ┆ 79312153  ┆ id99  ┆ id51861  ┆ id79312153  ┆ 73.818861 │
│ 11  ┆ 28655  ┆ 51482959  ┆ id11  ┆ id28655  ┆ id51482959  ┆ 66.362821 │
│ …   ┆ …      ┆ …         ┆ …     ┆ …        ┆ …           ┆ …         │
│ 13  ┆ 22767  ┆ 35069816  ┆ id13  ┆ id22767  ┆ id35069816  ┆ 94.651984 │
│ 72  ┆ 99663  ┆ 44320313  ┆ id72  ┆ id99663  ┆ id44320313  ┆ 32.654356 │
│ 110 ┆ 4985   ┆ 22435441  ┆ id110 ┆ id4985   ┆ id22435441  ┆ 75.312469 │
│ 17  ┆ 4136   ┆ 85575483  ┆ id17  ┆ id4136   ┆ id85575483  ┆ 63.577894 │
│ 70  ┆ 75769  ┆ 19286096  ┆ id70  ┆ id75769  ┆ id19286096  ┆ 49.151411 │
└─────┴────────┴───────────┴───────┴──────────┴─────────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id3': Int64, 'id4': String, 'id5': String, 'id6': String, 'v1': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e2_0_0.parquet
shape: (100, 3)
```
┌─────┬───────┬───────────┐
│ id1 ┆ id4   ┆ v2        │
│ --- ┆ ---   ┆ ---       │
│ i64 ┆ str   ┆ f64       │
╞═════╪═══════╪═══════════╡
│ 19  ┆ id19  ┆ 53.89299  │
│ 96  ┆ id96  ┆ 35.865322 │
│ 44  ┆ id44  ┆ 66.587577 │
│ 91  ┆ id91  ┆ 12.940303 │
│ 31  ┆ id31  ┆ 2.883551  │
│ …   ┆ …     ┆ …         │
│ 69  ┆ id69  ┆ 32.144187 │
│ 82  ┆ id82  ┆ 43.766849 │
│ 66  ┆ id66  ┆ 43.799275 │
│ 105 ┆ id105 ┆ 94.711328 │
│ 81  ┆ id81  ┆ 69.904453 │
└─────┴───────┴───────────┘
```
Schema({'id1': Int64, 'id4': String, 'v2': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e5_0_0.parquet
shape: (100_000, 5)
```
┌─────┬────────┬───────┬──────────┬───────────┐
│ id1 ┆ id2    ┆ id4   ┆ id5      ┆ v2        │
│ --- ┆ ---    ┆ ---   ┆ ---      ┆ ---       │
│ i64 ┆ i64    ┆ str   ┆ str      ┆ f64       │
╞═════╪════════╪═══════╪══════════╪═══════════╡
│ 69  ┆ 82839  ┆ id69  ┆ id82839  ┆ 79.322039 │
│ 94  ┆ 65192  ┆ id94  ┆ id65192  ┆ 26.282094 │
│ 27  ┆ 103858 ┆ id27  ┆ id103858 ┆ 51.550879 │
│ 10  ┆ 40683  ┆ id10  ┆ id40683  ┆ 84.647495 │
│ 42  ┆ 5979   ┆ id42  ┆ id5979   ┆ 83.488062 │
│ …   ┆ …      ┆ …     ┆ …        ┆ …         │
│ 42  ┆ 95337  ┆ id42  ┆ id95337  ┆ 32.217377 │
│ 104 ┆ 55177  ┆ id104 ┆ id55177  ┆ 39.670606 │
│ 14  ┆ 46220  ┆ id14  ┆ id46220  ┆ 55.6271   │
│ 31  ┆ 79430  ┆ id31  ┆ id79430  ┆ 52.355275 │
│ 60  ┆ 10612  ┆ id60  ┆ id10612  ┆ 64.503299 │
└─────┴────────┴───────┴──────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id4': String, 'id5': String, 'v2': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e8_0_0.parquet
shape: (100_000_000, 7)
```
┌─────┬───────┬──────────┬───────┬─────────┬────────────┬───────────┐
│ id1 ┆ id2   ┆ id3      ┆ id4   ┆ id5     ┆ id6        ┆ v2        │
│ --- ┆ ---   ┆ ---      ┆ ---   ┆ ---     ┆ ---        ┆ ---       │
│ i64 ┆ i64   ┆ i64      ┆ str   ┆ str     ┆ str        ┆ f64       │
╞═════╪═══════╪══════════╪═══════╪═════════╪════════════╪═══════════╡
│ 107 ┆ 53407 ┆ 81930178 ┆ id107 ┆ id53407 ┆ id81930178 ┆ 75.634881 │
│ 30  ┆ 44458 ┆ 73257490 ┆ id30  ┆ id44458 ┆ id73257490 ┆ 53.043222 │
│ 104 ┆ 89910 ┆ 38361265 ┆ id104 ┆ id89910 ┆ id38361265 ┆ 83.067211 │
│ 14  ┆ 74193 ┆ 47636586 ┆ id14  ┆ id74193 ┆ id47636586 ┆ 59.066146 │
│ 49  ┆ 77202 ┆ 95213755 ┆ id49  ┆ id77202 ┆ id95213755 ┆ 60.308764 │
│ …   ┆ …     ┆ …        ┆ …     ┆ …       ┆ …          ┆ …         │
│ 30  ┆ 26807 ┆ 87061377 ┆ id30  ┆ id26807 ┆ id87061377 ┆ 32.629123 │
│ 14  ┆ 73040 ┆ 40089145 ┆ id14  ┆ id73040 ┆ id40089145 ┆ 54.944554 │
│ 100 ┆ 49006 ┆ 98750911 ┆ id100 ┆ id49006 ┆ id98750911 ┆ 11.757998 │
│ 17  ┆ 89387 ┆ 16056323 ┆ id17  ┆ id89387 ┆ id16056323 ┆ 31.216292 │
│ 53  ┆ 67188 ┆ 12279067 ┆ id53  ┆ id67188 ┆ id12279067 ┆ 0.275111  │
└─────┴───────┴──────────┴───────┴─────────┴────────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id3': Int64, 'id4': String, 'id5': String, 'id6': String, 'v2': Float64})
```

### h2o join tables

The h2o join tables are ...

## ClickBench

TODO

## Polars Decision Support PDS-H

TODO
