#include "helpers.h"
#include "ypath_client.h"

#include <yt/core/misc/error.h>

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
public:
    virtual std::vector<TString> List() const override
    {
        std::vector<TString> keys;
        keys.reserve(Map_.size());
        for (const auto& pair : Map_) {
            keys.push_back(pair.first);
        }
        return keys;
    }

    virtual TYsonString FindYson(const TString& key) const override
    {
        auto it = Map_.find(key);
        return it == Map_.end() ? TYsonString() : TYsonString(it->second);
    }

    virtual void SetYson(const TString& key, const TYsonString& value) override
    {
        Y_ASSERT(value.GetType() == EYsonType::Node);
        Map_[key] = value.GetData();
    }

    virtual bool Remove(const TString& key) override
    {
        return Map_.erase(key) > 0;
    }

public:
    THashMap<TString, TString> Map_;

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
    virtual std::vector<TString> List() const override
    {
        return std::vector<TString>();
    }

    virtual TYsonString FindYson(const TString& key) const override
    {
        return TYsonString();
    }

    virtual void SetYson(const TString& key, const TYsonString& value) override
    {
        Y_UNREACHABLE();
    }

    virtual bool Remove(const TString& key) override
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
    auto keys = attributes.List();
    std::sort(keys.begin(), keys.end());
    consumer->OnBeginMap();
    for (const auto& key : keys) {
        consumer->OnKeyedItem(key);
        auto yson = attributes.GetYson(key);
        consumer->OnRaw(yson);
    }
    consumer->OnEndMap();
}

void ToProto(NProto::TAttributeDictionary* protoAttributes, const IAttributeDictionary& attributes)
{
    protoAttributes->Clear();
    auto keys = attributes.List();
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
        auto value = attributes.GetYson(key);
        auto protoAttribute = protoAttributes->add_attributes();
        protoAttribute->set_key(key);
        protoAttribute->set_value(value.GetData());
    }
}

std::unique_ptr<IAttributeDictionary> FromProto(const NProto::TAttributeDictionary& protoAttributes)
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
    std::sort(keys.begin(), keys.end());
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
        auto key = Load<TString>(context);
        auto value = Load<TYsonString>(context);
        obj.SetYson(key, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateYTreeKey(const TStringBuf& key)
{
    Y_UNUSED(key);
    // XXX(vvvv): Disabled due to existing data with empty keys, see https://st.yandex-team.ru/YQL-2640
#if 0
    if (key.empty()) {
        THROW_ERROR_EXCEPTION("Empty keys are not allowed in map nodes");
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
