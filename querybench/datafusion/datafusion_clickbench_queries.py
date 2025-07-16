from querybench.helpers import benchmark, get_results
import querybench.datafusion.clickbench_queries as clickbench_queries


def q0(ctx):
    return ctx.sql(clickbench_queries.q0()).collect()

def q1(ctx):
    return ctx.sql(clickbench_queries.q1()).collect()

def q2(ctx):
    return ctx.sql(clickbench_queries.q2()).collect()

def q3(ctx):
    return ctx.sql(clickbench_queries.q3()).collect()

def q4(ctx):
    return ctx.sql(clickbench_queries.q4()).collect()

def q5(ctx):
    return ctx.sql(clickbench_queries.q5()).collect()

def q6(ctx):
    return ctx.sql(clickbench_queries.q6()).collect()

def q7(ctx):
    return ctx.sql(clickbench_queries.q7()).collect()

def q8(ctx):
    return ctx.sql(clickbench_queries.q8()).collect()

def q9(ctx):
    return ctx.sql(clickbench_queries.q9()).collect()

def q10(ctx):
    return ctx.sql(clickbench_queries.q10()).collect()

def q11(ctx):
    return ctx.sql(clickbench_queries.q11()).collect()

def q12(ctx):
    return ctx.sql(clickbench_queries.q12()).collect()

def q13(ctx):
    return ctx.sql(clickbench_queries.q13()).collect()

def q14(ctx):
    return ctx.sql(clickbench_queries.q14()).collect()

def q15(ctx):
    return ctx.sql(clickbench_queries.q15()).collect()

def q16(ctx):
    return ctx.sql(clickbench_queries.q16()).collect()

def q17(ctx):
    return ctx.sql(clickbench_queries.q17()).collect()

def q18(ctx):
    return ctx.sql(clickbench_queries.q18()).collect()

def q19(ctx):
    return ctx.sql(clickbench_queries.q19()).collect()

def q20(ctx):
    return ctx.sql(clickbench_queries.q20()).collect()

def q21(ctx):
    return ctx.sql(clickbench_queries.q21()).collect()

def q22(ctx):
    return ctx.sql(clickbench_queries.q22()).collect()

def q23(ctx):
    return ctx.sql(clickbench_queries.q23()).collect()

def q24(ctx):
    return ctx.sql(clickbench_queries.q24()).collect()

def q25(ctx):
    return ctx.sql(clickbench_queries.q25()).collect()

def q26(ctx):
    return ctx.sql(clickbench_queries.q26()).collect()

def q27(ctx):
    return ctx.sql(clickbench_queries.q27()).collect()

def q28(ctx):
    return ctx.sql(clickbench_queries.q28()).collect()

def q29(ctx):
    return ctx.sql(clickbench_queries.q29()).collect()

def q30(ctx):
    return ctx.sql(clickbench_queries.q30()).collect()

def q31(ctx):
    return ctx.sql(clickbench_queries.q31()).collect()

def q32(ctx):
    return ctx.sql(clickbench_queries.q32()).collect()

def q33(ctx):
    return ctx.sql(clickbench_queries.q33()).collect()

def q34(ctx):
    return ctx.sql(clickbench_queries.q34()).collect()

def q35(ctx):
    return ctx.sql(clickbench_queries.q35()).collect()

def q36(ctx):
    return ctx.sql(clickbench_queries.q36()).collect()

def q37(ctx):
    return ctx.sql(clickbench_queries.q37()).collect()

def q38(ctx):
    return ctx.sql(clickbench_queries.q38()).collect()

def q39(ctx):
    return ctx.sql(clickbench_queries.q39()).collect()

def q40(ctx):
    return ctx.sql(clickbench_queries.q40()).collect()

def q41(ctx):
    return ctx.sql(clickbench_queries.q41()).collect()

def q42(ctx):
    return ctx.sql(clickbench_queries.q42()).collect()

def run_benchmarks(ctx):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q0, ctx, benchmarks=benchmarks, name="q0")
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
    benchmark(q11, ctx, benchmarks=benchmarks, name="q11")
    benchmark(q12, ctx, benchmarks=benchmarks, name="q12")
    benchmark(q13, ctx, benchmarks=benchmarks, name="q13")
    benchmark(q14, ctx, benchmarks=benchmarks, name="q14")
    benchmark(q15, ctx, benchmarks=benchmarks, name="q15")
    benchmark(q16, ctx, benchmarks=benchmarks, name="q16")
    benchmark(q17, ctx, benchmarks=benchmarks, name="q17")
    benchmark(q18, ctx, benchmarks=benchmarks, name="q18")
    benchmark(q19, ctx, benchmarks=benchmarks, name="q19")
    benchmark(q20, ctx, benchmarks=benchmarks, name="q20")
    benchmark(q21, ctx, benchmarks=benchmarks, name="q21")
    benchmark(q22, ctx, benchmarks=benchmarks, name="q22")
    benchmark(q23, ctx, benchmarks=benchmarks, name="q23")
    benchmark(q24, ctx, benchmarks=benchmarks, name="q24")
    benchmark(q25, ctx, benchmarks=benchmarks, name="q25")
    benchmark(q26, ctx, benchmarks=benchmarks, name="q26")
    benchmark(q27, ctx, benchmarks=benchmarks, name="q27")
    benchmark(q28, ctx, benchmarks=benchmarks, name="q28")
    benchmark(q29, ctx, benchmarks=benchmarks, name="q29")
    benchmark(q30, ctx, benchmarks=benchmarks, name="q30")
    benchmark(q31, ctx, benchmarks=benchmarks, name="q31")
    benchmark(q32, ctx, benchmarks=benchmarks, name="q32")
    benchmark(q33, ctx, benchmarks=benchmarks, name="q33")
    benchmark(q34, ctx, benchmarks=benchmarks, name="q34")
    benchmark(q35, ctx, benchmarks=benchmarks, name="q35")
    benchmark(q36, ctx, benchmarks=benchmarks, name="q36")
    benchmark(q37, ctx, benchmarks=benchmarks, name="q37")
    benchmark(q38, ctx, benchmarks=benchmarks, name="q38")
    benchmark(q39, ctx, benchmarks=benchmarks, name="q39")
    benchmark(q40, ctx, benchmarks=benchmarks, name="q40")
    benchmark(q41, ctx, benchmarks=benchmarks, name="q41")
    benchmark(q42, ctx, benchmarks=benchmarks, name="q42")


    res = get_results(benchmarks).set_index("task")
    return res
