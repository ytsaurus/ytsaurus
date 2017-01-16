#include <yt_udf.h>

long malloc_udf(TExpressionContext* context, long n)
{
    return (long)malloc(n);
}

