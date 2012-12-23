#include "stdafx.h"
#include "system_attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

ISystemAttributeProvider::TAttributeInfo* ISystemAttributeProvider::FindSystemAttributeInfo(const Stroka& key)
{
    std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
    ListSystemAttributes(&systemAttributes);
    auto it = std::find_if(
        systemAttributes.begin(),
        systemAttributes.end(),
        [&] (const ISystemAttributeProvider::TAttributeInfo& info) {
            return info.Key == key;
        });
    return it == systemAttributes.end() ? NULL : &(*it);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
