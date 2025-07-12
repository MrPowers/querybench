def q0():
    return 'SELECT COUNT(*) FROM hits;'

def q1():
    return 'SELECT COUNT(*) FROM hits WHERE "AdvEngineID" <> 0;'

def q2():
    return 'SELECT SUM("AdvEngineID"), COUNT(*), AVG("ResolutionWidth") FROM hits;'

def q3():
    return 'SELECT AVG("UserID") FROM hits;'

def q4():
    return 'SELECT COUNT(DISTINCT "UserID") FROM hits;'

def q5():
    return 'SELECT COUNT(DISTINCT "SearchPhrase") FROM hits;'

def q6():
    return 'SELECT MIN("EventDate"), MAX("EventDate") FROM hits;'

def q7():
    return 'SELECT "AdvEngineID", COUNT(*) FROM hits WHERE "AdvEngineID" <> 0 GROUP BY "AdvEngineID" ORDER BY COUNT(*) DESC;'

def q8():
    return 'SELECT "RegionID", COUNT(DISTINCT "UserID") AS u FROM hits GROUP BY "RegionID" ORDER BY u DESC LIMIT 10;'

def q9():
    return 'SELECT "RegionID", SUM("AdvEngineID"), COUNT(*) AS c, AVG("ResolutionWidth"), COUNT(DISTINCT "UserID") FROM hits GROUP BY "RegionID" ORDER BY c DESC LIMIT 10;'

def q10():
    return 'SELECT "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> \'\' GROUP BY "MobilePhoneModel" ORDER BY u DESC LIMIT 10;'

def q11():
    return 'SELECT "MobilePhone", "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> \'\' GROUP BY "MobilePhone", "MobilePhoneModel" ORDER BY u DESC LIMIT 10;'

def q12():
    return 'SELECT "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> \'\' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;'

def q13():
    return 'SELECT "SearchPhrase", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "SearchPhrase" <> \'\' GROUP BY "SearchPhrase" ORDER BY u DESC LIMIT 10;'

def q14():
    return 'SELECT "SearchEngineID", "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> \'\' GROUP BY "SearchEngineID", "SearchPhrase" ORDER BY c DESC LIMIT 10;'

def q15():
    return 'SELECT "UserID", COUNT(*) FROM hits GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10;'

def q16():
    return 'SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;'

def q17():
    return 'SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" LIMIT 10;'

def q18():
    return 'SELECT "UserID", extract(minute FROM to_timestamp_seconds("EventTime")) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;'

def q19():
    return 'SELECT "UserID" FROM hits WHERE "UserID" = 435090932899640449;'

def q20():
    return 'SELECT COUNT(*) FROM hits WHERE "URL" LIKE \'%google%\';'

def q21():
    return 'SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM hits WHERE "URL" LIKE \'%google%\' AND "SearchPhrase" <> \'\' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;'

def q22():
    return 'SELECT "SearchPhrase", MIN("URL"), MIN("Title"), COUNT(*) AS c, COUNT(DISTINCT "UserID") FROM hits WHERE "Title" LIKE \'%Google%\' AND "URL" NOT LIKE \'%.google.%\' AND "SearchPhrase" <> \'\' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;'

def q23():
    return 'SELECT * FROM hits WHERE "URL" LIKE \'%google%\' ORDER BY "EventTime" LIMIT 10;'

def q24():
    return 'SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> \'\' ORDER BY "EventTime" LIMIT 10;'

def q25():
    return 'SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> \'\' ORDER BY "SearchPhrase" LIMIT 10;'

def q26():
    return 'SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> \'\' ORDER BY "EventTime", "SearchPhrase" LIMIT 10;'

def q27():
    return 'SELECT "CounterID", AVG(length("URL")) AS l, COUNT(*) AS c FROM hits WHERE "URL" <> \'\' GROUP BY "CounterID" HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;'

def q28():
    return 'SELECT REGEXP_REPLACE("Referer", \'^https?://(?:www\\.)?([^/]+)/.*$\', \'\\1\') AS k, AVG(length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM hits WHERE "Referer" <> \'\' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;'

def q29():
    return """SELECT """ + ", ".join([f'SUM("ResolutionWidth" + {i})' for i in range(90)]) + ' FROM hits;'

def q30():
    return 'SELECT "SearchEngineID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> \'\' GROUP BY "SearchEngineID", "ClientIP" ORDER BY c DESC LIMIT 10;'

def q31():
    return 'SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> \'\' GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10;'

def q32():
    return 'SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10;'

def q33():
    return 'SELECT "URL", COUNT(*) AS c FROM hits GROUP BY "URL" ORDER BY c DESC LIMIT 10;'

def q34():
    return 'SELECT 1, "URL", COUNT(*) AS c FROM hits GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10;'

def q35():
    return 'SELECT "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3, COUNT(*) AS c FROM hits GROUP BY "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3 ORDER BY c DESC LIMIT 10;'

def q36():
    return 'SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "DontCountHits" = 0 AND "IsRefresh" = 0 AND "URL" <> \'\' GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10;'

def q37():
    return 'SELECT "Title", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "DontCountHits" = 0 AND "IsRefresh" = 0 AND "Title" <> \'\' GROUP BY "Title" ORDER BY PageViews DESC LIMIT 10;'

def q38():
    return 'SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "IsRefresh" = 0 AND "IsLink" <> 0 AND "IsDownload" = 0 GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;'

def q39():
    return 'SELECT "TraficSourceID", "SearchEngineID", "AdvEngineID", CASE WHEN ("SearchEngineID" = 0 AND "AdvEngineID" = 0) THEN "Referer" ELSE \'\' END AS Src, "URL" AS Dst, COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "IsRefresh" = 0 GROUP BY "TraficSourceID", "SearchEngineID", "AdvEngineID", Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;'

def q40():
    return 'SELECT "URLHash", "EventDate", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "IsRefresh" = 0 AND "TraficSourceID" IN (-1, 6) AND "RefererHash" = 3594120000172545465 GROUP BY "URLHash", "EventDate" ORDER BY PageViews DESC LIMIT 10 OFFSET 100;'

def q41():
    return 'SELECT "WindowClientWidth", "WindowClientHeight", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-01\' AND "EventDate" <= \'2013-07-31\' AND "IsRefresh" = 0 AND "DontCountHits" = 0 AND "URLHash" = 2868770270353813622 GROUP BY "WindowClientWidth", "WindowClientHeight" ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;'

def q42():
    return 'SELECT DATE_TRUNC(\'minute\', to_timestamp_seconds("EventTime")) AS M, COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= \'2013-07-14\' AND "EventDate" <= \'2013-07-15\' AND "IsRefresh" = 0 AND "DontCountHits" = 0 GROUP BY DATE_TRUNC(\'minute\', to_timestamp_seconds("EventTime")) ORDER BY DATE_TRUNC(\'minute\', M) LIMIT 10 OFFSET 1000;'
