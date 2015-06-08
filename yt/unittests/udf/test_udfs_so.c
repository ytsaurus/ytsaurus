#include <yt_udf.h>

int64_t abs_udf_so(TExecutionContext* context, int64_t n)
{
    return llabs(n);
}

int64_t exp_udf_so(TExecutionContext* context, int64_t n, int64_t m)
{
    int64_t result = 1;
    for (int64_t i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}

