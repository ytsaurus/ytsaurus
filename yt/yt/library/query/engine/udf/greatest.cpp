#include <yt/yt/library/query/misc/udf_cpp_abi.h>

extern "C" void Greatest(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* first,
    TUnversionedValue* args,
    int args_len);

extern "C" void greatest(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* first,
    TUnversionedValue* args,
    int args_len)
{
    Greatest(context, result, first, args, args_len);
}
