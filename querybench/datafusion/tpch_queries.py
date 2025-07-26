from querybench.helpers import benchmark, get_results
import querybench

def q1(ctx):
    return ctx.sql(querybench.queries.tpch.q1()).collect()


def q2(ctx):
    return ctx.sql(querybench.queries.tpch.q2()).collect()


def q3(ctx):
    return ctx.sql(querybench.queries.tpch.q3()).collect()


def q4(ctx):
    return ctx.sql(querybench.queries.tpch.q4()).collect()


def q5(ctx):
    return ctx.sql(querybench.queries.tpch.q5()).collect()


def q6(ctx):
    return ctx.sql(querybench.queries.tpch.q6()).collect()


def q7(ctx):
    return ctx.sql(querybench.queries.tpch.q7()).collect()


def q8(ctx):
    return ctx.sql(querybench.queries.tpch.q8()).collect()


def q9(ctx):
    return ctx.sql(querybench.queries.tpch.q9()).collect()


def q10(ctx):
    return ctx.sql(querybench.queries.tpch.q10()).collect()


def q11(ctx):
    return ctx.sql(querybench.queries.tpch.q11()).collect()


def q12(ctx):
    return ctx.sql(querybench.queries.tpch.q12()).collect()


def q13(ctx):
    return ctx.sql(querybench.queries.tpch.q13()).collect()


def q14(ctx):
    return ctx.sql(querybench.queries.tpch.q14()).collect()


def q15(ctx):
    return ctx.sql(querybench.queries.tpch.q15()).collect()


def q16(ctx):
    return ctx.sql(querybench.queries.tpch.q16()).collect()


def q17(ctx):
    return ctx.sql(querybench.queries.tpch.q17()).collect()


def q18(ctx):
    return ctx.sql(querybench.queries.tpch.q18()).collect()


def q19(ctx):
    return ctx.sql(querybench.queries.tpch.q19()).collect()


def q20(ctx):
    return ctx.sql(querybench.queries.tpch.q20()).collect()


def q21(ctx):
    return ctx.sql(querybench.queries.tpch.q21()).collect()


def q22(ctx):
    return ctx.sql(querybench.queries.tpch.q22()).collect()





def run_benchmarks(ctx):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, ctx, benchmarks=benchmarks, name="q1")
    benchmark(q2, ctx, benchmarks=benchmarks, name="q2")
    benchmark(q3, ctx, benchmarks=benchmarks, name="q3")
    benchmark(q4, ctx, benchmarks=benchmarks, name="q4")
    benchmark(q5, ctx, benchmarks=benchmarks, name="q5")
    benchmark(q6, ctx, benchmarks=benchmarks, name="q6")
    benchmark(q7, ctx, benchmarks=benchmarks, name="q7")
    benchmark(q8, ctx, benchmarks=benchmarks, name="q8")
    benchmark(q9, ctx, benchmarks=benchmarks, name="q9")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q10")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q11")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q12")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q13")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q14")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q15")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q16")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q17")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q18")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q19")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q20")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q21")
    benchmark(q10, ctx, benchmarks=benchmarks, name="q22")

    res = get_results(benchmarks).set_index("task")
    return res
