#include "system_attribute_provider.h"

#include <yt/core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void ISystemAttributeProvider::ReserveAndListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->reserve(64);
    ListSystemAttributes(descriptors);
}

void ISystemAttributeProvider::ListSystemAttributes(std::map<TInternedAttributeKey, TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> attributes;
    ReserveAndListSystemAttributes(&attributes);

    for (const auto& descriptor : attributes) {
        YCHECK(descriptors->insert(std::make_pair(descriptor.InternedKey, descriptor)).second);
    }
}

void ISystemAttributeProvider::ListBuiltinAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> systemAttributes;
    ReserveAndListSystemAttributes(&systemAttributes);

    for (const auto& attribute : systemAttributes) {
        if (!attribute.Custom) {
            (*descriptors).push_back(attribute);
        }
    }
}

TNullable<ISystemAttributeProvider::TAttributeDescriptor> ISystemAttributeProvider::FindBuiltinAttributeDescriptor(
    TInternedAttributeKey key)
{
    std::vector<TAttributeDescriptor> builtinAttributes;
    ListBuiltinAttributes(&builtinAttributes);

    auto it = std::find_if(
        builtinAttributes.begin(),
        builtinAttributes.end(),
        [&] (const ISystemAttributeProvider::TAttributeDescriptor& info) {
            // Suppress operator== overload for enums.
            return static_cast<int>(info.InternedKey) == static_cast<int>(key);
        });
    return it == builtinAttributes.end() ? Null : MakeNullable(*it);
}

TYsonString ISystemAttributeProvider::FindBuiltinAttribute(TInternedAttributeKey key)
{
    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    if (!GetBuiltinAttribute(key, &writer)) {
        return TYsonString();
    }
    writer.Flush();
    return TYsonString(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
