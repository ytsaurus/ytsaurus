#include <yt/ytlib/query_client/udf/udf_c_abi.h>

long malloc_udf(
    TExpressionContext* context,
    long n)
{
    (void)context;

    return (long)malloc(n);
}

