#include <yt/ytlib/query_client/udf/yt_udf_cpp.h>

#include <yp/server/objects/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatEnum(
    TExpressionContext* context,
    TUnversionedValue* result,
    const TUnversionedValue* arg)
{
    if (arg->Type != EValueType::Int64) {
        result->Type = EValueType::Null;
        return;
    }

    auto formattedValue = NYT::FormatEnum(static_cast<T>(arg->Data.Int64));
    result->Type = EValueType::String;
    result->Length = formattedValue.length();
    result->Data.String = AllocateBytes(context, result->Length);
    ::memcpy(result->Data.String, formattedValue.c_str(), result->Length);
}

#define DEFINE_ENUM_FORMATTER(type, Type)                       \
    extern "C" void format_##type(                              \
        TExpressionContext* context,                            \
        TUnversionedValue* result,                              \
        const TUnversionedValue* arg)                           \
    {                                                           \
        FormatEnum<Type>(context, result, arg);                 \
    }

DEFINE_ENUM_FORMATTER(pod_current_state, EPodCurrentState)
DEFINE_ENUM_FORMATTER(pod_target_state, EPodTargetState)
DEFINE_ENUM_FORMATTER(hfsm_state, EHfsmState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
