import duckdb
from querybench.helpers import benchmark, get_results
import sys
import pandas as pd

def duck_sql(sql, path):
    return sql.replace("hits", f"read_parquet('{path}')")

def q0(path):
    sql = "SELECT COUNT(*) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q1(path):
    sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q2(path):
    sql = "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q3(path):
    sql = "SELECT AVG(UserID) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q4(path):
    sql = "SELECT COUNT(DISTINCT UserID) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q5(path):
    sql = "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q6(path):
    sql = "SELECT MIN(EventDate), MAX(EventDate) FROM hits;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q7(path):
    sql = "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q8(path):
    sql = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q9(path):
    sql = "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q10(path):
    sql = "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q11(path):
    sql = "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q12(path):
    sql = "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q13(path):
    sql = "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q14(path):
    sql = "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q15(path):
    sql = "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q16(path):
    sql = "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q17(path):
    sql = "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q18(path):
    sql = "SELECT UserID, EXTRACT(MINUTE FROM TO_TIMESTAMP(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q19(path):
    sql = "SELECT UserID FROM hits WHERE UserID = 435090932899640449;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q20(path):
    sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';"
    return duckdb.sql(duck_sql(sql, path)).df()

def q21(path):
    sql = "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q22(path):
    sql = "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q23(path):
    sql = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q24(path):
    sql = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q25(path):
    sql = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q26(path):
    sql = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q27(path):
    sql = "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q28(path):
    sql = r"""SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"""
    return duckdb.sql(duck_sql(sql, path)).df()

def q29(path):
    sql = """
    SELECT 
        SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), 
        SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), 
        SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), 
        SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), 
        SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), 
        SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), 
        SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), 
        SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), 
        SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), 
        SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), 
        SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), 
        SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), 
        SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), 
        SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), 
        SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), 
        SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), 
        SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), 
        SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) 
    FROM hits;
    """
    return duckdb.sql(duck_sql(sql, path)).df()

def q30(path):
    sql = "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q31(path):
    sql = "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q32(path):
    sql = "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q33(path):
    sql = "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q34(path):
    sql = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q35(path):
    sql = "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q36(path):
    sql = "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q37(path):
    sql = "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q38(path):
    sql = "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q39(path):
    sql = "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q40(path):
    sql = "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q41(path):
    sql = "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND make_date(EventDate) >= '2013-07-01' AND make_date(EventDate) <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;"
    return duckdb.sql(duck_sql(sql, path)).df()

def q42(path):
    sql = ""
    return duckdb.sql(duck_sql(sql, path)).df()

def run_benchmarks(path):
    duckdb_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q0, dfs=path, benchmarks=duckdb_benchmarks, name="q0")
    benchmark(q1, dfs=path, benchmarks=duckdb_benchmarks, name="q1")
    benchmark(q2, dfs=path, benchmarks=duckdb_benchmarks, name="q2")
    benchmark(q3, dfs=path, benchmarks=duckdb_benchmarks, name="q3")
    benchmark(q4, dfs=path, benchmarks=duckdb_benchmarks, name="q4")
    benchmark(q5, dfs=path, benchmarks=duckdb_benchmarks, name="q5")
    benchmark(q6, dfs=path, benchmarks=duckdb_benchmarks, name="q6")
    benchmark(q7, dfs=path, benchmarks=duckdb_benchmarks, name="q7")
    benchmark(q8, dfs=path, benchmarks=duckdb_benchmarks, name="q8")
    benchmark(q9, dfs=path, benchmarks=duckdb_benchmarks, name="q9")
    benchmark(q10, dfs=path, benchmarks=duckdb_benchmarks, name="q10")
    benchmark(q10, dfs=path, benchmarks=duckdb_benchmarks, name="q10")
    benchmark(q11, dfs=path, benchmarks=duckdb_benchmarks, name="q11")
    benchmark(q12, dfs=path, benchmarks=duckdb_benchmarks, name="q12")
    benchmark(q13, dfs=path, benchmarks=duckdb_benchmarks, name="q13")
    benchmark(q14, dfs=path, benchmarks=duckdb_benchmarks, name="q14")
    benchmark(q15, dfs=path, benchmarks=duckdb_benchmarks, name="q15")
    benchmark(q16, dfs=path, benchmarks=duckdb_benchmarks, name="q16")
    benchmark(q17, dfs=path, benchmarks=duckdb_benchmarks, name="q17")
    benchmark(q18, dfs=path, benchmarks=duckdb_benchmarks, name="q18")
    benchmark(q19, dfs=path, benchmarks=duckdb_benchmarks, name="q19")
    benchmark(q20, dfs=path, benchmarks=duckdb_benchmarks, name="q20")
    benchmark(q21, dfs=path, benchmarks=duckdb_benchmarks, name="q21")
    benchmark(q22, dfs=path, benchmarks=duckdb_benchmarks, name="q22")
    benchmark(q23, dfs=path, benchmarks=duckdb_benchmarks, name="q23")
    benchmark(q24, dfs=path, benchmarks=duckdb_benchmarks, name="q24")
    benchmark(q25, dfs=path, benchmarks=duckdb_benchmarks, name="q25")
    benchmark(q26, dfs=path, benchmarks=duckdb_benchmarks, name="q26")
    benchmark(q27, dfs=path, benchmarks=duckdb_benchmarks, name="q27")
    benchmark(q28, dfs=path, benchmarks=duckdb_benchmarks, name="q28")
    benchmark(q29, dfs=path, benchmarks=duckdb_benchmarks, name="q29")
    benchmark(q30, dfs=path, benchmarks=duckdb_benchmarks, name="q30")
    benchmark(q31, dfs=path, benchmarks=duckdb_benchmarks, name="q31")
    benchmark(q32, dfs=path, benchmarks=duckdb_benchmarks, name="q32")
    benchmark(q33, dfs=path, benchmarks=duckdb_benchmarks, name="q33")
    benchmark(q34, dfs=path, benchmarks=duckdb_benchmarks, name="q34")
    benchmark(q35, dfs=path, benchmarks=duckdb_benchmarks, name="q35")
    benchmark(q36, dfs=path, benchmarks=duckdb_benchmarks, name="q36")
    benchmark(q37, dfs=path, benchmarks=duckdb_benchmarks, name="q37")
    benchmark(q38, dfs=path, benchmarks=duckdb_benchmarks, name="q38")
    benchmark(q39, dfs=path, benchmarks=duckdb_benchmarks, name="q39")
    benchmark(q40, dfs=path, benchmarks=duckdb_benchmarks, name="q40")
    benchmark(q41, dfs=path, benchmarks=duckdb_benchmarks, name="q41")
    # benchmark(q42, dfs=path, benchmarks=duckdb_benchmarks, name="q42")


    duckdb_res_temp = get_results(duckdb_benchmarks).set_index("task")
    print(duckdb_res_temp)
    return duckdb_res_temp
