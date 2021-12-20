#include "object_id.h"

#include <yt/yt/core/misc/format.h>

#include <util/system/type_name.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

TEnumValueIndex::operator size_t() const
{
    auto hash = std::hash<std::type_index>()(EnumType);

    THash<TString> strHash;
    hash = CombineHashes(hash, strHash(EnumName));
    return CombineHashes(hash, strHash(ValueName));
}

void FormatValue(TStringBuilderBase* builder, const TEnumValueIndex& index, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v::%v", index.EnumName, index.ValueName);
}

TString ToString(const TEnumValueIndex& index)
{
    return Format("%v", index);
}

////////////////////////////////////////////////////////////////////////////////

TObjectId::operator size_t() const
{
    auto hash = std::hash<std::type_index>()(Type);
    if (Kind) {
        hash = CombineHashes(hash, THash<TEnumValueIndex>()(*Kind));
    }
    if (Name) {
        hash = CombineHashes(hash, THash<TString>()(*Name));
    }
    return hash;
}

TString TObjectId::TypeName() const
{
    return ::TypeName(Type);
}

void FormatValue(TStringBuilderBase* builder, const TObjectId& id, TStringBuf /*spec*/)
{
    builder->AppendString(id.TypeName());
    if (id.Kind) {
        builder->AppendFormat("(%v)", *id.Kind);
    }

    if (id.Name) {
        builder->AppendFormat("(\"%v\")", *id.Name);
    }
}

TString ToString(const TObjectId& id)
{
    return Format("%v", id);
}

TObjectId TObjectId::GetTypeId() const
{
    return TObjectId{Type};
}

TObjectId TObjectId::CastTo(TObjectId other) const
{
    TObjectId result = other;
    result.Kind = Kind;
    result.Name = Name;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI
