#include "stdafx.h"
#include "attributes.h"

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
