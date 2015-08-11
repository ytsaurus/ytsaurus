#include "stdafx.h"
#include "system_attribute_provider.h"

#include <core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void ISystemAttributeProvider::ListSystemAttributes(std::map<Stroka, TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> list;
    ListSystemAttributes(&list);
    for (const auto& descriptor : list) {
        YCHECK(descriptors->insert(std::make_pair(Stroka(descriptor.Key), descriptor)).second);
    }
}

void ISystemAttributeProvider::ListBuiltinAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> systemAttributes;
    ListSystemAttributes(&systemAttributes);

    for (const auto& attribute : systemAttributes) {
        if (!attribute.Custom) {
            (*descriptors).push_back(attribute);
        }
    }
}

TNullable<ISystemAttributeProvider::TAttributeDescriptor> ISystemAttributeProvider::FindBuiltinAttributeDescriptor(
    const Stroka& key)
{
    std::vector<TAttributeDescriptor> builtinAttributes;
    ListBuiltinAttributes(&builtinAttributes);
    auto it = std::find_if(
        builtinAttributes.begin(),
        builtinAttributes.end(),
        [&] (const ISystemAttributeProvider::TAttributeDescriptor& info) {
            return info.Key == key;
        });
    return it == builtinAttributes.end() ? Null : MakeNullable(*it);
}

TNullable<TYsonString> ISystemAttributeProvider::GetBuiltinAttribute(const Stroka& key)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary, EYsonType::Node, true);
    if (!GetBuiltinAttribute(key, &writer)) {
        return Null;
    }
    return TYsonString(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
