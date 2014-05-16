#include "stdafx.h"
#include "system_attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

ISystemAttributeProvider::TAttributeInfo* ISystemAttributeProvider::FindBuiltinAttributeInfo(const Stroka& key)
{
    std::vector<TAttributeInfo> systemAttributes;
    ListBuiltinAttributes(&systemAttributes);
    auto it = std::find_if(
        systemAttributes.begin(),
        systemAttributes.end(),
        [&] (const ISystemAttributeProvider::TAttributeInfo& info) {
            return info.Key == key;
        });
    return it == systemAttributes.end() ? NULL : &(*it);
}

void ISystemAttributeProvider::ListBuiltinAttributes(std::vector<TAttributeInfo>* attributes)
{
    std::vector<TAttributeInfo> systemAttributes;
    ListSystemAttributes(&systemAttributes);

    for (const auto& attribute : systemAttributes) {
        if (!attribute.IsCustom) {
            (*attributes).push_back(attribute);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
