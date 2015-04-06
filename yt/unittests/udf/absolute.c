#include <stdlib.h>
#include <udf_helpers.h>

long absolute(TExecutionContext* context, long n)
{
    return labs(n);
}
