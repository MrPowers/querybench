from querybench.helpers import benchmark, get_results
import querybench


def q1(spark):
    return spark.sql(querybench.queries.tpch.q1()).collect()


def q2(spark):
    return spark.sql(querybench.queries.tpch.q2()).collect()


def q3(spark):
    return spark.sql(querybench.queries.tpch.q3()).collect()


def q4(spark):
    return spark.sql(querybench.queries.tpch.q4()).collect()


def q5(spark):
    return spark.sql(querybench.queries.tpch.q5()).collect()


def q6(spark):
    return spark.sql(querybench.queries.tpch.q6()).collect()


def q7(spark):
    return spark.sql(querybench.queries.tpch.q7()).collect()


def q8(spark):
    return spark.sql(querybench.queries.tpch.q8()).collect()


def q9(spark):
    return spark.sql(querybench.queries.tpch.q9()).collect()


def q10(spark):
    return spark.sql(querybench.queries.tpch.q10()).collect()


def q11(spark):
    return spark.sql(querybench.queries.tpch.q11()).collect()


def q12(spark):
    return spark.sql(querybench.queries.tpch.q12()).collect()


def q13(spark):
    return spark.sql(querybench.queries.tpch.q13()).collect()


def q14(spark):
    return spark.sql(querybench.queries.tpch.q14()).collect()


def q15(spark):
    return spark.sql(querybench.queries.tpch.q15()).collect()


def q16(spark):
    return spark.sql(querybench.queries.tpch.q16()).collect()


def q17(spark):
    return spark.sql(querybench.queries.tpch.q17()).collect()


def q18(spark):
    return spark.sql(querybench.queries.tpch.q18()).collect()


def q19(spark):
    return spark.sql(querybench.queries.tpch.q19()).collect()


def q20(spark):
    return spark.sql(querybench.queries.tpch.q20()).collect()


def q21(spark):
    return spark.sql(querybench.queries.tpch.q21()).collect()


def q22(spark):
    return spark.sql(querybench.queries.tpch.q22()).collect()





def run_benchmarks(spark):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, spark, benchmarks=benchmarks, name="q1")
    benchmark(q2, spark, benchmarks=benchmarks, name="q2")
    benchmark(q3, spark, benchmarks=benchmarks, name="q3")
    benchmark(q4, spark, benchmarks=benchmarks, name="q4")
    benchmark(q5, spark, benchmarks=benchmarks, name="q5")
    benchmark(q6, spark, benchmarks=benchmarks, name="q6")
    benchmark(q7, spark, benchmarks=benchmarks, name="q7")
    benchmark(q8, spark, benchmarks=benchmarks, name="q8")
    benchmark(q9, spark, benchmarks=benchmarks, name="q9")
    benchmark(q10, spark, benchmarks=benchmarks, name="q10")
    benchmark(q11, spark, benchmarks=benchmarks, name="q11")
    benchmark(q12, spark, benchmarks=benchmarks, name="q12")
    benchmark(q13, spark, benchmarks=benchmarks, name="q13")
    benchmark(q14, spark, benchmarks=benchmarks, name="q14")
    # benchmark(q15, spark, benchmarks=benchmarks, name="q15")
    benchmark(q16, spark, benchmarks=benchmarks, name="q16")
    benchmark(q17, spark, benchmarks=benchmarks, name="q17")
    benchmark(q18, spark, benchmarks=benchmarks, name="q18")
    benchmark(q19, spark, benchmarks=benchmarks, name="q19")
    benchmark(q20, spark, benchmarks=benchmarks, name="q20")
    benchmark(q21, spark, benchmarks=benchmarks, name="q21")
    benchmark(q22, spark, benchmarks=benchmarks, name="q22")

    res = get_results(benchmarks).set_index("task")
    return res
