#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <cmath>

using namespace NYT::NQueryClient::NUdf;

extern "C" char is_finite(
    TExpressionContext* /*context*/,
    double value)
{
    return std::isfinite(value);
}
