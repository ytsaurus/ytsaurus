#include "type_tag.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

const TTypeTag<TString> NameTag{"name"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

template <>
TString ToString(const NRoren::TDynamicTypeTag& tag)
{
    TStringStream out;
    out << "TDynamicTag{" << tag.GetRowVtable().TypeName << ", " << tag.GetDescription() << "}";
    return out.Str();
}
