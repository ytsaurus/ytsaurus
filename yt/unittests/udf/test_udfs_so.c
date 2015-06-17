#include <yt_udf.h>

int64_t abs_udf(TExecutionContext* context, int64_t n)
{
    return llabs(n);
}

int64_t abs_udf_so(TExecutionContext* context, int64_t n)
{
    return llabs(n);
}

int64_t exp_udf(TExecutionContext* context, int64_t n, int64_t m)
{
    int64_t result = 1;
    for (int64_t i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}

uint64_t max_udaf_init(
    TExecutionContext* context)
{
    return 0;
}

static uint64_t max_udaf_iteration(
    TExecutionContext* context,
    uint64_t state,
    uint64_t newValue)
{
    if (state < newValue) {
        return newValue;
    } else {
        return state;
    }
}

uint64_t max_udaf_update(
    TExecutionContext* context,
    uint64_t state,
    uint64_t newValue)
{
    return max_udaf_iteration(context, state, newValue);
}

uint64_t max_udaf_merge(
    TExecutionContext* context,
    uint64_t dstState,
    uint64_t state)
{
    return max_udaf_iteration(context, dstState, state);
}

uint64_t max_udaf_finalize(
    TExecutionContext* context,
    uint64_t state)
{
    return state;
}
