#include "stdafx.h"
#include "attributes.h"
#include "ytree.h"
#include "ephemeral.h"
#include "serialize.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYson IAttributeDictionary::GetYson(const Stroka& key) const
{
    const auto& result = FindYson(key);
    if (!result) {
        ythrow yexception() << Sprintf("Attribute %s is not found", ~key.Quote());
    }
    return *result;
}

IMapNodePtr IAttributeDictionary::ToMap() const
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    auto keys = List();
    FOREACH (const auto& key, keys) {
        auto value = DeserializeFromYson(GetYson(key));
        map->AddChild(~value, key);
    }
    return map;
}

TAutoPtr<IAttributeDictionary> IAttributeDictionary::FromMap(IMapNodePtr node)
{
    auto attributes = CreateEphemeralAttributes();
    FOREACH (const auto& pair, node->GetChildren()) {
        attributes->SetYson(pair.first, SerializeToYson(pair.second));
    }
    return attributes;
}

void IAttributeDictionary::MergeFrom(const IMapNodePtr other)
{
    FOREACH (const auto& pair, other->GetChildren()) {
        const auto& key = pair.first;
        auto value = SerializeToYson(~pair.second);
        SetYson(key, value);
    }
}

void IAttributeDictionary::MergeFrom(const IAttributeDictionary& other)
{
    FOREACH (const auto& key, other.List()) {
        auto value = other.GetYson(key);
        SetYson(key, value);
    }
}

void IAttributeDictionary::Clear()
{
    auto keys = List();
    FOREACH (const auto& key, keys) {
        Remove(key);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeDictionary
    : public IAttributeDictionary
{
    typedef yhash_map<Stroka, TYPath> TAttributeMap;
    TAttributeMap Map;

    virtual yhash_set<Stroka> List() const
    {
        yhash_set<Stroka> keys;
        FOREACH (const auto& pair, Map) {
            keys.insert(pair.first);
        }
        return keys;
    }

    virtual TNullable<TYson> FindYson(const Stroka& key) const
    {
        auto it = Map.find(key);
        return it == Map.end() ? Null : MakeNullable(it->second);
    }

    virtual void SetYson(const Stroka& key, const TYson& value)
    {
        Map[key] = value;
    }

    virtual bool Remove(const Stroka& key)
    {
        return Map.erase(key) > 0;
    }
};

TAutoPtr<IAttributeDictionary> CreateEphemeralAttributes()
{
    return new TEphemeralAttributeDictionary();
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyAttributeDictionary
    : public IAttributeDictionary
{
    virtual yhash_set<Stroka> List() const
    {
        return yhash_set<Stroka>();
    }

    virtual TNullable<TYson> FindYson(const Stroka& key) const
    {
        return Null;
    }

    virtual void SetYson(const Stroka& key, const TYson& value)
    {
        YUNREACHABLE();
    }

    virtual bool Remove(const Stroka& key)
    {
        return false;
    }
};

const IAttributeDictionary& EmptyAttributes()
{
    return *Singleton<TEmptyAttributeDictionary>();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TAttributes* protoAttributes, const IAttributeDictionary& attributes)
{
    FOREACH (const auto& key, attributes.List()) {
        auto value = attributes.GetYson(key);
        auto protoAttribute = protoAttributes->add_attributes();
        protoAttribute->set_key(key);
        protoAttribute->set_value(value);
    }
}

TAutoPtr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes)
{
    auto attributes = CreateEphemeralAttributes();
    FOREACH (const auto& protoAttribute, protoAttributes.attributes()) {
        const auto& key = protoAttribute.key();
        const auto& value = protoAttribute.value();
        attributes->SetYson(key, value);
    }
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
