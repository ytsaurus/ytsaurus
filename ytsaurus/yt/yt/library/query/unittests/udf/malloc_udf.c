#include <yt/yt/library/query/misc/udf_c_abi.h>

long malloc_udf(
    TExpressionContext* context,
    long n)
{
    (void)context;

    return (long)malloc(n);
}

