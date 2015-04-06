#include <udf_helpers.h>

long exponentiate(TExecutionContext* context, long n, long m)
{
    long result = 1;
    for (long i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}
