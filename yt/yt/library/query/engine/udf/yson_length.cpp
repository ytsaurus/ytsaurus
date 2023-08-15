#include <yt/yt/library/query/misc/udf_c_abi.h>

extern "C" int64_t YsonLength(char* data, int length);

extern "C" int64_t yson_length(
    TExpressionContext* /*context*/,
    char* data,
    int length)
{
    return YsonLength(data, length);
}
