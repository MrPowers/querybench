import daft
from helpers import benchmark, get_results


def q0(df):
    hits = df
    return daft.sql("SELECT COUNT(*) FROM hits;").collect()

def q1(df):
    hits = df
    return daft.sql("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;").collect()

def q2(df):
    hits = df
    return daft.sql("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;").collect()

def q3(df):
    hits = df
    return daft.sql("SELECT AVG(UserID) FROM hits;").collect()

def q4(df):
    hits = df
    return daft.sql("SELECT COUNT(DISTINCT UserID) FROM hits;").collect()

def q5(df):
    hits = df
    return daft.sql("SELECT COUNT(DISTINCT SearchPhrase) FROM hits;").collect()

def q6(df):
    hits = df
    return daft.sql("SELECT MIN(EventDate) as m1, MAX(EventDate) as m2 FROM hits;").collect()

def q7(df):
    hits = df
    return daft.sql("SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;").collect()

def q8(df):
    hits = df
    return daft.sql("SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;").collect()

def q9(df):
    hits = df
    return daft.sql("SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;").collect()

def q10(df):
    hits = df
    return daft.sql("SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;").collect()

def q11(df):
    hits = df
    return daft.sql("SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;").collect()

def q12(df):
    hits = df
    return daft.sql("SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;").collect()

def q13(df):
    hits = df
    return daft.sql("SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;").collect()

def q14(df):
    hits = df
    return daft.sql("SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;").collect()

def q15(df):
    hits = df
    return daft.sql("SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;").collect()

def q16(df):
    hits = df
    return daft.sql("SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;").collect()

def q17(df):
    hits = df
    return daft.sql("SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;").collect()

def q18(df):
    hits = df
    return daft.sql("SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;").collect()

def q19(df):
    hits = df
    return daft.sql("SELECT UserID FROM hits WHERE UserID = 435090932899640449;").collect()

def q20(df):
    hits = df
    return daft.sql("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';").collect()

def q21(df):
    hits = df
    return daft.sql("SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;").collect()

def q22(df):
    hits = df
    return daft.sql("SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;").collect()

def q23(df):
    hits = df
    return daft.sql("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;").collect()

def q24(df):
    hits = df
    return daft.sql("SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;").collect()

def q25(df):
    hits = df
    return daft.sql("SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;").collect()

def q26(df):
    hits = df
    return daft.sql("SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;").collect()

def q27(df):
    hits = df
    return daft.sql("SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;").collect()

def q28(df):
    hits = df
    return daft.sql(r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) as m FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;").collect()

def q29(df):
    hits = df
    return daft.sql(
        "SELECT " +
        ", ".join([f"SUM(ResolutionWidth + {i}) AS s{i}" for i in range(90)]) +
        " FROM hits;"
    ).collect()

def q30(df):
    hits = df
    return daft.sql("SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;").collect()

def q31(df):
    hits = df
    return daft.sql("SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;").collect()

def q32(df):
    hits = df
    return daft.sql("SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;").collect()

def q33(df):
    hits = df
    return daft.sql("SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;").collect()

def q34(df):
    hits = df
    return daft.sql("SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;").collect()

def q35(df):
    hits = df
    return daft.sql("SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;").collect()

def q36(df):
    hits = df
    return daft.sql("SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;").collect()

def q37(df):
    hits = df
    return daft.sql("SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;").collect()

def q38(df):
    hits = df
    return daft.sql("SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 1010;").collect()

def q39(df):
    hits = df
    return daft.sql("SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 1010;").collect()

def q40(df):
    hits = df
    return daft.sql("SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 110;").collect()

def q41(df):
    hits = df
    return daft.sql("SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10010;").collect()

def q42(df):
    hits = df
    return daft.sql("SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 1010;").collect()


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
    # benchmark(q18, df, benchmarks=benchmarks, name="q18")
    benchmark(q19, df, benchmarks=benchmarks, name="q19")
    benchmark(q20, df, benchmarks=benchmarks, name="q20")
    benchmark(q21, df, benchmarks=benchmarks, name="q21")
    benchmark(q22, df, benchmarks=benchmarks, name="q22")
    benchmark(q23, df, benchmarks=benchmarks, name="q23")
    benchmark(q24, df, benchmarks=benchmarks, name="q24")
    benchmark(q25, df, benchmarks=benchmarks, name="q25")
    benchmark(q26, df, benchmarks=benchmarks, name="q26")
    benchmark(q27, df, benchmarks=benchmarks, name="q27")
    benchmark(q28, df, benchmarks=benchmarks, name="q28")
    benchmark(q29, df, benchmarks=benchmarks, name="q29")
    benchmark(q30, df, benchmarks=benchmarks, name="q30")
    benchmark(q31, df, benchmarks=benchmarks, name="q31")
    benchmark(q32, df, benchmarks=benchmarks, name="q32")
    benchmark(q33, df, benchmarks=benchmarks, name="q33")
    benchmark(q34, df, benchmarks=benchmarks, name="q34")
    # benchmark(q35, df, benchmarks=benchmarks, name="q35")
    # benchmark(q36, df, benchmarks=benchmarks, name="q36")
    # benchmark(q37, df, benchmarks=benchmarks, name="q37")
    # benchmark(q38, df, benchmarks=benchmarks, name="q38")
    # benchmark(q39, df, benchmarks=benchmarks, name="q39")
    # benchmark(q40, df, benchmarks=benchmarks, name="q40")
    # benchmark(q41, df, benchmarks=benchmarks, name="q41")
    # benchmark(q42, df, benchmarks=benchmarks, name="q42")


    res = get_results(benchmarks).set_index("task")
    return res
