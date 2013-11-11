#include "stdafx.h"
#include "attribute_helpers.h"

#include <core/misc/error.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool operator == (const IAttributeDictionary& lhs, const IAttributeDictionary& rhs)
{
    auto lhsKeys = lhs.List();
    std::sort(lhsKeys.begin(), lhsKeys.end());

    auto rhsKeys = rhs.List();
    std::sort(rhsKeys.begin(), rhsKeys.end());

    if (lhsKeys != rhsKeys) {
        return false;
    }

    for (const auto& key : lhsKeys) {
        auto lhsValue = lhs.Get<INodePtr>(key);
        auto rhsValue = rhs.Get<INodePtr>(key);
        if (!AreNodesEqual(lhsValue, rhsValue)) {
            return false;
        }
    }

    return true;
}

bool operator != (const IAttributeDictionary& lhs, const IAttributeDictionary& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeDictionary
    : public IAttributeDictionary
{
    typedef yhash_map<Stroka, TYPath> TAttributeMap;
    TAttributeMap Map;

    virtual std::vector<Stroka> List() const override
    {
        std::vector<Stroka> keys;
        for (const auto& pair : Map) {
            keys.push_back(pair.first);
        }
        return keys;
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        auto it = Map.find(key);
        return it == Map.end() ? Null : MakeNullable(TYsonString(it->second));
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        YASSERT(value.GetType() == EYsonType::Node);
        Map[key] = value.Data();
    }

    virtual bool Remove(const Stroka& key) override
    {
        return Map.erase(key) > 0;
    }
};

std::unique_ptr<IAttributeDictionary> CreateEphemeralAttributes()
{
    return std::unique_ptr<IAttributeDictionary>(new TEphemeralAttributeDictionary());
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyAttributeDictionary
    : public IAttributeDictionary
{
public:
    virtual std::vector<Stroka> List() const override
    {
        return std::vector<Stroka>();
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        return Null;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        YUNREACHABLE();
    }

    virtual bool Remove(const Stroka& key) override
    {
        return false;
    }
};

const IAttributeDictionary& EmptyAttributes()
{
    return *Singleton<TEmptyAttributeDictionary>();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const IAttributeDictionary& attributes, IYsonConsumer* consumer)
{
    auto list = attributes.List();
    consumer->OnBeginMap();
    for (const auto& key : list) {
        consumer->OnKeyedItem(key);
        auto yson = attributes.GetYson(key);
        consumer->OnRaw(yson.Data(), yson.GetType());
    }
    consumer->OnEndMap();
}

void ToProto(NProto::TAttributes* protoAttributes, const IAttributeDictionary& attributes)
{
    protoAttributes->Clear();
    for (const auto& key : attributes.List()) {
        auto value = attributes.GetYson(key);
        auto protoAttribute = protoAttributes->add_attributes();
        protoAttribute->set_key(key);
        protoAttribute->set_value(value.Data());
    }
}

std::unique_ptr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes)
{
    auto attributes = CreateEphemeralAttributes();
    for (const auto& protoAttribute : protoAttributes.attributes()) {
        const auto& key = protoAttribute.key();
        const auto& value = protoAttribute.value();
        attributes->SetYson(key, TYsonString(value));
    }
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

void TAttributeDictionaryValueSerializer::Save(TStreamSaveContext& context, const IAttributeDictionary& obj)
{
    using NYT::Save;
    auto keys = obj.List();
    TSizeSerializer::Save(context, keys.size());
    for (const auto& key : keys) {
        Save(context, key);
        Save(context, obj.GetYson(key));
    }
}

void TAttributeDictionaryValueSerializer::Load(TStreamLoadContext& context, IAttributeDictionary& obj)
{
    using NYT::Load;
    obj.Clear();
    size_t size = TSizeSerializer::Load(context);
    for (size_t index = 0; index < size; ++index) {
        auto key = Load<Stroka>(context);
        auto value = Load<TYsonString>(context);
        obj.SetYson(key, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAttributeDictionaryRefSerializer::Save(TStreamSaveContext& context, const std::unique_ptr<IAttributeDictionary>& obj)
{
    using NYT::Save;
    if (obj) {
        Save(context, true);
        Save(context, *obj);
    } else {
        Save(context, false);
    }
}

void TAttributeDictionaryRefSerializer::Load(TStreamLoadContext& context, std::unique_ptr<IAttributeDictionary>& obj)
{
    using NYT::Load;
    if (Load<bool>(context)) {
        obj = CreateEphemeralAttributes();
        Load(context, *obj);
    } else {
        obj.reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
