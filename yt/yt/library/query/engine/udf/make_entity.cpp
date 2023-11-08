#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void make_entity(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    result->Type = EValueType::Any;
    result->Length = 1;
    result->Data.String = AllocateBytes(context, 1);
    result->Data.String[0] = '#';
}
