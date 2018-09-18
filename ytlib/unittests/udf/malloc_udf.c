#include <yt/ytlib/query_client/udf/yt_udf.h>

long malloc_udf(TExpressionContext* context, long n)
{
    (void)context;
    return (long)malloc(n);
}

