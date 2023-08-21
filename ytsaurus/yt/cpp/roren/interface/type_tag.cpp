#include "type_tag.h"

template <>
TString ToString(const NRoren::TDynamicTypeTag& tag)
{
    TStringStream out;
    out << "TDynamicTag{" << tag.GetRowVtable().TypeName << ", " << tag.GetDescription() << "}";
    return out.Str();
}
