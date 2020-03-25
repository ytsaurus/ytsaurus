#include "helpers.h"
#include "ypath_client.h"

#include <yt/core/misc/error.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool operator == (const IAttributeDictionary& lhs, const IAttributeDictionary& rhs)
{
    auto lhsPairs = lhs.ListPairs();
    auto rhsPairs = rhs.ListPairs();
    if (lhsPairs.size() != rhsPairs.size()) {
        return false;
    }

    std::sort(lhsPairs.begin(), lhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    std::sort(rhsPairs.begin(), rhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    for (auto index = 0; index < lhsPairs.size(); ++index) {
        if (lhsPairs[index].first != rhsPairs[index].first) {
            return false;
        }
    }

    for (auto index = 0; index < lhsPairs.size(); ++index) {
        auto lhsNode = ConvertToNode(lhsPairs[index].second);
        auto rhsNode = ConvertToNode(rhsPairs[index].second);
        if (!AreNodesEqual(lhsNode, rhsNode)) {
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
    virtual std::vector<TString> ListKeys() const override
    {
        std::vector<TString> keys;
        keys.reserve(Map_.size());
        for (const auto& pair : Map_) {
            keys.push_back(pair.first);
        }
        return keys;
    }

    virtual std::vector<TKeyValuePair> ListPairs() const override
    {
        std::vector<TKeyValuePair> pairs;
        pairs.reserve(Map_.size());
        for (const auto& pair : Map_) {
            pairs.push_back(pair);
        }
        return pairs;
    }

    virtual TYsonString FindYson(TStringBuf key) const override
    {
        auto it = Map_.find(key);
        return it == Map_.end() ? TYsonString() : it->second;
    }

    virtual void SetYson(const TString& key, const TYsonString& value) override
    {
        YT_ASSERT(value.GetType() == EYsonType::Node);
        Map_[key] = value;
    }

    virtual bool Remove(const TString& key) override
    {
        return Map_.erase(key) > 0;
    }

private:
    THashMap<TString, TYsonString> Map_;

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
    virtual std::vector<TString> ListKeys() const override
    {
        return {};
    }

    virtual std::vector<TKeyValuePair> ListPairs() const override
    {
        return {};
    }

    virtual TYsonString FindYson(TStringBuf /*key*/) const override
    {
        return {};
    }

    virtual void SetYson(const TString& /*key*/, const TYsonString& /*value*/) override
    {
        YT_ABORT();
    }

    virtual bool Remove(const TString& /*key*/) override
    {
        return false;
    }
};

const IAttributeDictionary& EmptyAttributes()
{
    return *Singleton<TEmptyAttributeDictionary>();
}

////////////////////////////////////////////////////////////////////////////////

class TThreadSafeAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    explicit TThreadSafeAttributeDictionary(IAttributeDictionary* underlying)
        : Underlying_(underlying)
    { }

    virtual std::vector<TString> ListKeys() const override
    {
        NConcurrency::TReaderGuard guard(Lock_);
        return Underlying_->ListKeys();
    }

    virtual std::vector<TKeyValuePair> ListPairs() const override
    {
        NConcurrency::TReaderGuard guard(Lock_);
        return Underlying_->ListPairs();
    }

    virtual NYson::TYsonString FindYson(TStringBuf key) const override
    {
        NConcurrency::TReaderGuard guard(Lock_);
        return Underlying_->FindYson(key);
    }

    virtual void SetYson(const TString& key, const NYson::TYsonString& value) override
    {
        NConcurrency::TWriterGuard guard(Lock_);
        Underlying_->SetYson(key, value);
    }

    virtual bool Remove(const TString& key) override
    {
        NConcurrency::TWriterGuard guard(Lock_);
        return Underlying_->Remove(key);
    }

private:
    IAttributeDictionary* const Underlying_;
    NConcurrency::TReaderWriterSpinLock Lock_;
};

std::unique_ptr<IAttributeDictionary> CreateThreadSafeAttributes(IAttributeDictionary* underlying)
{
    return std::unique_ptr<IAttributeDictionary>(new TThreadSafeAttributeDictionary(underlying));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const IAttributeDictionary& attributes, IYsonConsumer* consumer)
{
    auto pairs = attributes.ListPairs();
    std::sort(pairs.begin(), pairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    consumer->OnBeginMap();
    for (const auto& [key, value] : pairs) {
        consumer->OnKeyedItem(key);
        consumer->OnRaw(value);
    }
    consumer->OnEndMap();
}

void ToProto(NProto::TAttributeDictionary* protoAttributes, const IAttributeDictionary& attributes)
{
    protoAttributes->Clear();
    auto pairs = attributes.ListPairs();
    std::sort(pairs.begin(), pairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    protoAttributes->mutable_attributes()->Reserve(static_cast<int>(pairs.size()));
    for (const auto& [key, value] : pairs) {
        auto* protoAttribute = protoAttributes->add_attributes();
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
    auto pairs = obj.ListPairs();
    std::sort(pairs.begin(), pairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    TSizeSerializer::Save(context, pairs.size());
    for (const auto& [key, value] : pairs) {
        Save(context, key);
        Save(context, value);
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

void ValidateYTreeKey(TStringBuf key)
{
    Y_UNUSED(key);
    // XXX(vvvv): Disabled due to existing data with empty keys, see https://st.yandex-team.ru/YQL-2640
#if 0
    if (key.empty()) {
        THROW_ERROR_EXCEPTION("Empty keys are not allowed in map nodes");
    }
#endif
}

void ValidateYPathResolutionDepth(const TYPath& path, int depth)
{
    if (depth > MaxYPathResolveIterations) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Path %v exceeds resolve depth limit",
            path)
            << TErrorAttribute("limit", MaxYPathResolveIterations);
    }
}

std::vector<IAttributeDictionary::TKeyValuePair> ListAttributesPairs(const IAttributeDictionary& attributes)
{
    std::vector<IAttributeDictionary::TKeyValuePair> result;
    auto keys = attributes.ListKeys();
    result.reserve(keys.size());
    for (const auto& key : keys) {
        auto value = attributes.FindYson(key);
        if (value) {
            result.push_back(std::make_pair(key, value));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
