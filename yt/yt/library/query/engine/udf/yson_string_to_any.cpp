#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <string.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void ValidateYsonHelper(char*, uint32_t);

extern "C" void yson_string_to_any(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* yson)
{
    ValidateYsonHelper(yson->Data.String, yson->Length);

    result->Type = EValueType::Any;
    result->Length = yson->Length;
    result->Data.String = AllocateBytes(context, result->Length);
    memcpy(result->Data.String, yson->Data.String, result->Length);
}
