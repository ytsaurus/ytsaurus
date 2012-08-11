#include "stdafx.h"
#include "attributes.h"

#include "ytree.h"
#include "ephemeral.h"
#include "attribute_consumer.h"
#include "yson_parser.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary::~IAttributeDictionary()
{ }

TYsonString IAttributeDictionary::GetYson(const Stroka& key) const
{
    const auto& result = FindYson(key);
    if (!result) {
        ythrow yexception() << Sprintf("Attribute %s is not found", ~key.Quote());
    }
    return *result;
}

void IAttributeDictionary::MergeFrom(const IMapNodePtr other)
{
    FOREACH (const auto& pair, other->GetChildren()) {
        const auto& key = pair.first;
        auto value = ConvertToYsonString(pair.second);
        SetYson(key, value);
    }
}

void IAttributeDictionary::MergeFrom(const IAttributeDictionary& other)
{
    FOREACH (const auto& key, other.List()) {
        TYsonString value = other.GetYson(key);
        SetYson(key, value);
    }
}

TAutoPtr<IAttributeDictionary> IAttributeDictionary::Clone()
{
    auto attributes = CreateEphemeralAttributes();
    attributes->MergeFrom(*this);
    return attributes;
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

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const
    {
        auto it = Map.find(key);
        return it == Map.end() ? Null : MakeNullable(TYsonString(it->second));
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value)
    {
        YASSERT(value.GetType() == EYsonType::Node);
        Map[key] = value.Data();
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
public:
    virtual yhash_set<Stroka> List() const
    {
        return yhash_set<Stroka>();
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const
    {
        return Null;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value)
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
        protoAttribute->set_value(value.Data());
    }
}

TAutoPtr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes)
{
    auto attributes = CreateEphemeralAttributes();
    FOREACH (const auto& protoAttribute, protoAttributes.attributes()) {
        const auto& key = protoAttribute.key();
        const auto& value = protoAttribute.value();
        attributes->SetYson(key, TYsonString(value));
    }
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const IAttributeDictionary& attributes, IYsonConsumer* consumer)
{
    auto list = attributes.List();
    consumer->OnBeginMap();
    FOREACH (const auto& key, list) {
        consumer->OnKeyedItem(key);
        auto yson = attributes.GetYson(key);
        consumer->OnRaw(yson.Data(), yson.GetType());
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
