#include "stdafx.h"
#include "attributes.h"

#include "ytree.h"
#include "ephemeral.h"
#include "serialize.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TTestConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TTestConfig> TPtr;
};

TYson IAttributeDictionary::GetYson(const Stroka& name)
{
    const auto& result = FindYson(name);
    if (!result) {
        ythrow yexception() << Sprintf("Attribute %s is not found", ~name.Quote());
    }
    return *result;
}

IMapNode::TPtr IAttributeDictionary::ToMap()
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    auto names = List();
    FOREACH (const auto& name, names) {
        auto value = DeserializeFromYson(GetYson(name));
        map->AddChild(~value, name);
    }
    return map;
}

void IAttributeDictionary::MergeFrom(const IMapNode* map)
{
    FOREACH (const auto& pair, map->GetChildren()) {
        const auto& key = pair.first;
        auto value = SerializeToYson(~pair.second);
        SetYson(key, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TInMemoryAttributeDictionary
    : public IAttributeDictionary
{
    typedef yhash_map<Stroka, TYPath> TAttributeMap;
    TAttributeMap Map;

    virtual yhash_set<Stroka> List()
    {
        yhash_set<Stroka> names;
        FOREACH (const auto& pair, Map) {
            names.insert(pair.first);
        }
        return names;
    }

    virtual TNullable<TYson> FindYson(const Stroka& name)
    {
        auto it = Map.find(name);
        return it == Map.end() ? TNullable<TYson>() : it->second;
    }

    virtual void SetYson(const Stroka& name, const TYson& value)
    {
        Map[name] = value;
    }

    virtual bool Remove(const Stroka& name)
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
