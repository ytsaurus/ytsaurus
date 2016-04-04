#include <yt_udf.h>

long malloc_udf(TExecutionContext* context, long n)
{
    return (long)malloc(n);
}

