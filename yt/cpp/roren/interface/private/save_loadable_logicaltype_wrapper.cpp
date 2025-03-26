#include "save_loadable_logicaltype_wrapper.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TSaveLoadableLogicalTypeWrapper::Save(IOutputStream* output) const
{
    bool hasValue = static_cast<bool>(Value);
    ::Save(output, hasValue);
    if (hasValue) {
        ::NYT::NYson::TYsonString str = ::NYT::NYson::ConvertToYsonString(Value);
        ::Save(output, str);
    }
}

void TSaveLoadableLogicalTypeWrapper::Load(IInputStream* input)
{
    bool hasValue = false;
    ::Load(input, hasValue);
    if (hasValue) {
        ::NYT::NYson::TYsonString str;
        ::Load(input, str);
        Value = ::NYT::ConvertTo<::NYT::NTableClient::TLogicalTypePtr>(str);
    } else {
        Value = {};
    }
}

TSaveLoadableLogicalTypeWrapper& SaveLoadableLogicalType(::NYT::NTableClient::TLogicalTypePtr& value)
{
    return *reinterpret_cast<TSaveLoadableLogicalTypeWrapper*>(&value);
}

const TSaveLoadableLogicalTypeWrapper& SaveLoadableLogicalType(const ::NYT::NTableClient::TLogicalTypePtr& value)
{
    return *reinterpret_cast<const TSaveLoadableLogicalTypeWrapper*>(&value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

