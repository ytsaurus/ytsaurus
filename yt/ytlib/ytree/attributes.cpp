#include "stdafx.h"
#include "attributes.h"

#include "ytree.h"
#include "ephemeral.h"
#include "serialize.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYson IAttributeDictionary::GetAttribute(const Stroka& name)
{
    const auto& result = FindAttribute(name);
    if (result.empty()) {
        ythrow yexception() << Sprintf("Attribute %s is not found", ~name.Quote());
    }
    return result;
}

IMapNode::TPtr IAttributeDictionary::ToMap()
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    auto names = ListAttributes();
    FOREACH (const auto& name, names) {
        auto value = DeserializeFromYson(GetAttribute(name));
        map->AddChild(~value, name);
    }
    return map;
}

void IAttributeDictionary::Merge(const IMapNode* map)
{
    FOREACH (const auto& pair, map->GetChildren()) {
        const auto& key = pair.first;
        auto value = SerializeToYson(~pair.second);
        SetAttribute(key, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TInMemoryAttributeDictionary
    : public IAttributeDictionary
{
    typedef yhash_map<Stroka, TYPath> TAttributeMap;
    TAttributeMap Map;

    virtual yhash_set<Stroka> ListAttributes()
    {
        yhash_set<Stroka> names;
        FOREACH (const auto& pair, Map) {
            names.insert(pair.first);
        }
        return names;
    }

    virtual TYson FindAttribute(const Stroka& name)
    {
        auto it = Map.find(name);
        return it == Map.end() ? TYson() : it->second;
    }

    virtual void SetAttribute(const Stroka& name, const TYson& value)
    {
        Map[name] = value;
    }

    virtual bool RemoveAttribute(const Stroka& name)
    {
        return Map.erase(name) > 0;
    }
};

IAttributeDictionary::TPtr CreateInMemoryAttributeDictionary()
{
    return New<TInMemoryAttributeDictionary>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
