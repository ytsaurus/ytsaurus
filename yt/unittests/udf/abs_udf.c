#include <stdlib.h>
#include <udf_helpers.h>

long abs_udf(TExecutionContext* context, long n)
{
    return labs(n);
}
