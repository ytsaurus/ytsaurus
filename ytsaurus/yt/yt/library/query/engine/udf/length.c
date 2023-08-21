#include <yt/yt/library/query/misc/udf_c_abi.h>

int64_t length(
    TExpressionContext* context,
    char* s,
    int length)
{
    (void)context;
    (void)s;

    return length;
}
