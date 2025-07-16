import polars as pl
from querybench.helpers import benchmark, get_results
from datetime import date


def q0(x):
    return x.select(pl.len()).collect(engine="streaming").height


def q1(x):
    return x.select(pl.col("AdvEngineID").filter(pl.col("AdvEngineID") != 0).count()).collect(engine="streaming").height


def q2(x):
    return x.select(a_sum=pl.col("AdvEngineID").sum(), count=pl.len(), a_mean=pl.col("AdvEngineID").mean()).collect(engine="streaming").rows()[0]


def q3(x):
    return x.select(pl.col("UserID").mean()).collect(engine="streaming").item()


def q4(x):
    return x.select(pl.col("UserID").n_unique()).collect(engine="streaming").item()


def q5(x):
    return x.select(pl.col("SearchPhrase").n_unique()).collect(engine="streaming").item()


def q6(x):
    return x.select(e_min=pl.col("EventDate").min(), e_max=pl.col("EventDate").max()).collect(engine="streaming").rows()[0]


def q7(x):
    return (x.filter(pl.col("AdvEngineID") != 0)
        .group_by("AdvEngineID")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True).collect(engine="streaming"))


def q8(x):
    return (
        x.group_by("RegionID")
        .agg(pl.col("UserID").n_unique())
        .sort("UserID", descending=True)
        .head(10).collect(engine="streaming")
    )


def q9(x):
    return (
        x.group_by("RegionID")
        .agg(
            [
                pl.sum("AdvEngineID").alias("AdvEngineID_sum"),
                pl.mean("ResolutionWidth").alias("ResolutionWidth_mean"),
                pl.col("UserID").n_unique().alias("UserID_nunique"),
            ]
        )
        .sort("AdvEngineID_sum", descending=True)
        .head(10).collect(engine="streaming")
    )


def q10(x):
    return (
        x.filter(pl.col("MobilePhoneModel") != "")
        .group_by("MobilePhoneModel")
        .agg(pl.col("UserID").n_unique())
        .sort("UserID", descending=True)
        .head(10).collect(engine="streaming")
    )


def q11(x):
    return (
        x.filter(pl.col("MobilePhoneModel") != "")
        .group_by(["MobilePhone", "MobilePhoneModel"])
        .agg(pl.col("UserID").n_unique())
        .sort("UserID", descending=True)
        .head(10).collect(engine="streaming")
    )


def q12(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .group_by("SearchPhrase")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q13(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .group_by("SearchPhrase")
        .agg(pl.col("UserID").n_unique())
        .sort("UserID", descending=True)
        .head(10).collect(engine="streaming")
    )


def q14(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .group_by(["SearchEngineID", "SearchPhrase"])
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q15(x):
    return (
        x.group_by("UserID")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q16(x):
    return (
        x.group_by(["UserID", "SearchPhrase"])
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q17(x):
    return (
        x.group_by(["UserID", "SearchPhrase"]).agg(pl.len()).head(10).collect(engine="streaming")
    )


def q18(x):
    return (
        x.group_by(
            [pl.col("UserID"), pl.col("EventTime").dt.minute(), "SearchPhrase"]
        )
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q19(x):
    return (
        x.select("UserID").filter(pl.col("UserID") == 435090932899640449).collect(engine="streaming")
    )


def q20(x):
    return (
        x.filter(pl.col("URL").str.contains("google")).select(pl.len()).collect(engine="streaming").item()
    )


def q21(x):
    return (
        x.filter(
            (pl.col("URL").str.contains("google")) & (pl.col("SearchPhrase") != "")
        )
        .group_by("SearchPhrase")
        .agg([pl.col("URL").min(), pl.len().alias("count")])
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q22(x):
    return (
        x.filter(
            (pl.col("Title").str.contains("Google"))
            & (~pl.col("URL").str.contains(".google."))
            & (pl.col("SearchPhrase") != "")
        )
        .group_by("SearchPhrase")
        .agg(
            [
                pl.col("URL").min(),
                pl.col("Title").min(),
                pl.len().alias("count"),
                pl.col("UserID").n_unique(),
            ]
        )
        .sort("count", descending=True)
        .head(10).collect(engine="streaming")
    )


def q23(x):
    return (
        x.filter(pl.col("URL").str.contains("google"))
        .sort("EventTime")
        .head(10).collect(engine="streaming")
    )


def q24(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .sort("EventTime")
        .select("SearchPhrase")
        .head(10).collect(engine="streaming")
    )



def q25(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .sort("SearchPhrase")
        .select("SearchPhrase")
        .head(10).collect(engine="streaming")
    )


def q26(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .sort(["EventTime", "SearchPhrase"])
        .select("SearchPhrase")
        .head(10).collect(engine="streaming")
    )


def q27(x):
    return (
        x.filter(pl.col("URL") != "")  # WHERE URL <> ''
        .group_by("CounterID")  # GROUP BY CounterID
        .agg(
            [
                pl.col("URL").str.len_chars().mean().alias("l"),  # AVG(STRLEN(URL))
                pl.len().alias("c"),  # COUNT(*)
            ]
        )
        .filter(pl.col("c") > 100000)  # HAVING COUNT(*) > 100000
        .sort("l", descending=True)  # ORDER BY l DESC
        .limit(25).collect(engine="streaming")  # LIMIT 25
    )

def q28(x):
    return (
        x.filter(pl.col("Referer") != "")
            .with_columns(
                pl.col("Referer")
                .str.extract(r"^https?://(?:www\\.)?([^/]+)/.*$")
                .alias("k")
            )
            .group_by("k")
            .agg(
                [
                    pl.col("Referer").str.len_chars().mean().alias("l"),  # AVG(STRLEN(Referer))
                    pl.col("Referer").min().alias("min_referer"),  # MIN(Referer)
                    pl.len().alias("c"),  # COUNT(*)
                ]
            )
            .filter(pl.col("c") > 100000)  # HAVING COUNT(*) > 100000
            .sort("l", descending=True)  # ORDER BY l DESC
            .limit(25).collect(engine="streaming")  # LIMIT 25
    )

def q29(x):
    return (
        x.select(pl.sum_horizontal([pl.col("ResolutionWidth").shift(i) for i in range(1, 90)])).collect(engine="streaming")
    )

def q30(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .group_by(["SearchEngineID", "ClientIP"])
        .agg(
            [
                pl.len().alias("c"),
                pl.sum("IsRefresh").alias("IsRefreshSum"),
                pl.mean("ResolutionWidth").alias("AvgResolutionWidth"),
            ]
        )
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )


def q31(x):
    return (
        x.filter(pl.col("SearchPhrase") != "")
        .group_by(["WatchID", "ClientIP"])
        .agg(
            [
                pl.len().alias("c"),
                pl.sum("IsRefresh").alias("IsRefreshSum"),
                pl.mean("ResolutionWidth").alias("AvgResolutionWidth"),
            ]
        )
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )

def q32(x):
    return (
        x.group_by(["WatchID", "ClientIP"])
        .agg(
            [
                pl.len().alias("c"),
                pl.sum("IsRefresh").alias("IsRefreshSum"),
                pl.mean("ResolutionWidth").alias("AvgResolutionWidth"),
            ]
        )
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )


def q33(x):
    return (
        x.group_by("URL")
        .agg(pl.len().alias("c"))
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )


def q34(x):
    return (
        x.group_by("URL")
        .agg(pl.len().alias("c"))
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )

def q35(x):
    return (
        x.with_columns([pl.col("ClientIP")])
        .group_by(["ClientIP"])
        .agg(pl.len().alias("c"))
        .sort("c", descending=True)
        .head(10).collect(engine="streaming")
    )

def q36(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("DontCountHits") == 0)
            & (pl.col("IsRefresh") == 0)
            & (pl.col("URL") != "")
        )
        .group_by("URL")
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .head(10).collect(engine="streaming")
    )


def q37(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("DontCountHits") == 0)
            & (pl.col("IsRefresh") == 0)
            & (pl.col("Title") != "")
        )
        .group_by("Title")
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .head(10).collect(engine="streaming")
    )

def q38(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("IsRefresh") == 0)
            & (pl.col("IsLink") != 0)
            & (pl.col("IsDownload") == 0)
        )
        .group_by("URL")
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .slice(1000, 10).collect(engine="streaming")
    )

def q39(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("IsRefresh") == 0)
        )
        .group_by(
            [
                "TraficSourceID",
                "SearchEngineID",
                "AdvEngineID",
                pl.when(pl.col("SearchEngineID").eq(0) & pl.col("AdvEngineID").eq(0))
                .then(pl.col("Referer"))
                .otherwise(pl.lit(""))
                .alias("Src"),
                "URL",
            ]
        )
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .slice(1000, 10).collect(engine="streaming")
    )

def q40(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("IsRefresh") == 0)
            & (pl.col("TraficSourceID").is_in([-1, 6]))
            & (pl.col("RefererHash") == 3594120000172545465)
        )
        .group_by(["URLHash", "EventDate"])
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .slice(100, 10).collect(engine="streaming")
    )

def q41(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 1))
            & (pl.col("EventDate") <= date(2013, 7, 31))
            & (pl.col("IsRefresh") == 0)
            & (pl.col("DontCountHits") == 0)
            & (pl.col("URLHash") == 2868770270353813622)
        )
        .group_by(["WindowClientWidth", "WindowClientHeight"])
        .agg(pl.len().alias("PageViews"))
        .sort("PageViews", descending=True)
        .slice(10000, 10).collect(engine="streaming")
    )

def q42(x):
    return (
        x.filter(
            (pl.col("CounterID") == 62)
            & (pl.col("EventDate") >= date(2013, 7, 14))
            & (pl.col("EventDate") <= date(2013, 7, 15))
            & (pl.col("IsRefresh") == 0)
            & (pl.col("DontCountHits") == 0)
        )
        .group_by(pl.col("EventTime").dt.truncate("1m"))
        .agg(pl.len().alias("PageViews"))
        .slice(1000, 10).collect(engine="streaming")
    )



def run_benchmarks(df):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q0, df, benchmarks=benchmarks, name="q0")
    benchmark(q1, df, benchmarks=benchmarks, name="q1")
    benchmark(q2, df, benchmarks=benchmarks, name="q2")
    benchmark(q3, df, benchmarks=benchmarks, name="q3")
    benchmark(q4, df, benchmarks=benchmarks, name="q4")
    benchmark(q5, df, benchmarks=benchmarks, name="q5")
    benchmark(q6, df, benchmarks=benchmarks, name="q6")
    benchmark(q7, df, benchmarks=benchmarks, name="q7")
    benchmark(q8, df, benchmarks=benchmarks, name="q8")
    benchmark(q9, df, benchmarks=benchmarks, name="q9")
    benchmark(q10, df, benchmarks=benchmarks, name="q10")
    benchmark(q11, df, benchmarks=benchmarks, name="q11")
    benchmark(q12, df, benchmarks=benchmarks, name="q12")
    benchmark(q13, df, benchmarks=benchmarks, name="q13")
    benchmark(q14, df, benchmarks=benchmarks, name="q14")
    benchmark(q15, df, benchmarks=benchmarks, name="q15")
    benchmark(q16, df, benchmarks=benchmarks, name="q16")
    benchmark(q17, df, benchmarks=benchmarks, name="q17")
    benchmark(q18, df, benchmarks=benchmarks, name="q18")
    benchmark(q19, df, benchmarks=benchmarks, name="q19")
    benchmark(q20, df, benchmarks=benchmarks, name="q20")
    benchmark(q21, df, benchmarks=benchmarks, name="q21")
    benchmark(q22, df, benchmarks=benchmarks, name="q22")
    benchmark(q23, df, benchmarks=benchmarks, name="q23")
    benchmark(q24, df, benchmarks=benchmarks, name="q24")
    benchmark(q25, df, benchmarks=benchmarks, name="q25")
    benchmark(q26, df, benchmarks=benchmarks, name="q26")
    benchmark(q27, df, benchmarks=benchmarks, name="q27")
    # benchmark(q28, df, benchmarks=benchmarks, name="q28")
    benchmark(q29, df, benchmarks=benchmarks, name="q29")
    benchmark(q30, df, benchmarks=benchmarks, name="q30")
    benchmark(q31, df, benchmarks=benchmarks, name="q31")
    benchmark(q32, df, benchmarks=benchmarks, name="q32")
    benchmark(q33, df, benchmarks=benchmarks, name="q33")
    benchmark(q34, df, benchmarks=benchmarks, name="q34")
    benchmark(q35, df, benchmarks=benchmarks, name="q35")
    benchmark(q36, df, benchmarks=benchmarks, name="q36")
    benchmark(q37, df, benchmarks=benchmarks, name="q37")
    benchmark(q38, df, benchmarks=benchmarks, name="q38")
    benchmark(q39, df, benchmarks=benchmarks, name="q39")
    benchmark(q40, df, benchmarks=benchmarks, name="q40")
    benchmark(q41, df, benchmarks=benchmarks, name="q41")
    benchmark(q42, df, benchmarks=benchmarks, name="q42")

    res = get_results(benchmarks).set_index("task")
    return res
