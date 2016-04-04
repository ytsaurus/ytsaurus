#include <yt_udf.h>

#include <stdlib.h>

int64_t abs_udf(TExpressionContext* context, int64_t n)
{
    return llabs(n);
}

int64_t abs_udf_o(TExpressionContext* context, int64_t n)
{
    return llabs(n);
}

int64_t exp_udf(TExpressionContext* context, int64_t n, int64_t m)
{
    int64_t result = 1;
    for (int64_t i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}

uint64_t max_udaf_init(
    TExpressionContext* context)
{
    return 0;
}

static uint64_t max_udaf_iteration(
    TExpressionContext* context,
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
    TExpressionContext* context,
    uint64_t state,
    uint64_t newValue)
{
    return max_udaf_iteration(context, state, newValue);
}

uint64_t max_udaf_merge(
    TExpressionContext* context,
    uint64_t dstState,
    uint64_t state)
{
    return max_udaf_iteration(context, dstState, state);
}

uint64_t max_udaf_finalize(
    TExpressionContext* context,
    uint64_t state)
{
    return state;
}
