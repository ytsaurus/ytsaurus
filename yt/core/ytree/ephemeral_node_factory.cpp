#include "ephemeral_node_factory.h"
#include "ephemeral_attribute_owner.h"
#include "node_detail.h"
#include "ypath_client.h"
#include "ypath_detail.h"

#include <yt/core/misc/hash.h>
#include <yt/core/misc/singleton.h>

#include <yt/core/yson/async_consumer.h>

#include <algorithm>

namespace NYT::NYTree {

using namespace NRpc;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeBase
    : public TNodeBase
    , public TSupportsAttributes
    , public TEphemeralAttributeOwner
{
public:
    explicit TEphemeralNodeBase(bool shouldHideAttributes)
        : ShouldHideAttributes_(shouldHideAttributes)
    { }

    virtual std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        return CreateEphemeralNodeFactory(ShouldHideAttributes_);
    }

    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent_.Lock();
    }

    virtual void SetParent(const ICompositeNodePtr& parent) override
    {
        Y_ASSERT(!parent || Parent_.IsExpired());
        Parent_ = parent;
    }

    virtual bool ShouldHideAttributes() override
    {
        return ShouldHideAttributes_;
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override
    {
        if (!HasAttributes()) {
            return;
        }

        const auto& attributes = Attributes();

        auto keys = attributes.List();
        if (stable) {
            std::sort(keys.begin(), keys.end());
        }

        THashSet<TString> matchingKeys;
        if (attributeKeys) {
            matchingKeys = THashSet<TString>(attributeKeys->begin(), attributeKeys->end());
        }

        for (const auto& key : keys) {
            if (!attributeKeys || matchingKeys.find(key) != matchingKeys.end()) {
                auto yson = attributes.GetYson(key);
                consumer->OnKeyedItem(key);
                consumer->OnRaw(yson);
            }
        }
    }

protected:
    // TSupportsAttributes members
    virtual IAttributeDictionary* GetCustomAttributes() override
    {
        return MutableAttributes();
    }

private:
    TWeakPtr<ICompositeNode> Parent_;
    bool ShouldHideAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase>
class TScalarNode
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
    TScalarNode(bool shouldHideAttributes)
        : TEphemeralNodeBase(shouldHideAttributes)
        , Value_()
    { }

    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const override
    {
        return Value_;
    }

    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) override
    {
        Value_ = value;
    }

private:
    TValue Value_;
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
        YTREE_NODE_TYPE_OVERRIDES(name) \
        \
        public: \
            T ## name ## Node (bool shouldHideAttributes) \
                : TScalarNode<type, I ## name ## Node> (shouldHideAttributes) \
            { } \
    };

DECLARE_SCALAR_TYPE(String, TString)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Uint64, ui64)
DECLARE_SCALAR_TYPE(Double, double)
DECLARE_SCALAR_TYPE(Boolean, bool)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase>
class TCompositeNodeBase
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
    TCompositeNodeBase(bool shouldHideAttributes)
        : TEphemeralNodeBase(shouldHideAttributes)
    { }

    virtual TIntrusivePtr<ICompositeNode> AsComposite() override
    {
        return this;
    }

    virtual TIntrusivePtr<const ICompositeNode> AsComposite() const override
    {
        return this;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCompositeNodeBase<IMapNode>
    , public TMapNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(Map)

public:
    explicit TMapNode(bool shouldHideAttributes)
        : TCompositeNodeBase<IMapNode>(shouldHideAttributes)
    { }

    virtual void Clear() override
    {
        for (const auto& pair : KeyToChild_) {
            pair.second->SetParent(nullptr);
        }
        KeyToChild_.clear();
        ChildToKey_.clear();
    }

    virtual int GetChildCount() const override
    {
        return KeyToChild_.ysize();
    }

    virtual std::vector< std::pair<TString, INodePtr> > GetChildren() const override
    {
        return std::vector< std::pair<TString, INodePtr> >(KeyToChild_.begin(), KeyToChild_.end());
    }

    virtual std::vector<TString> GetKeys() const override
    {
        std::vector<TString> result;
        result.reserve(KeyToChild_.size());
        for (const auto& pair : KeyToChild_) {
            result.push_back(pair.first);
        }
        return result;
    }

    virtual INodePtr FindChild(const TString& key) const override
    {
        auto it = KeyToChild_.find(key);
        return it == KeyToChild_.end() ? nullptr : it->second;
    }

    virtual bool AddChild(const TString& key, const INodePtr& child) override
    {
        Y_ASSERT(child);
        ValidateYTreeKey(key);

        if (KeyToChild_.insert(std::make_pair(key, child)).second) {
            YCHECK(ChildToKey_.emplace(child, key).second);
            child->SetParent(this);
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const TString& key) override
    {
        auto it = KeyToChild_.find(TString(key));
        if (it == KeyToChild_.end())
            return false;

        auto child = it->second;
        child->SetParent(nullptr);
        KeyToChild_.erase(it);
        YCHECK(ChildToKey_.erase(child) == 1);

        return true;
    }

    virtual void RemoveChild(const INodePtr& child) override
    {
        Y_ASSERT(child);

        child->SetParent(nullptr);

        auto it = ChildToKey_.find(child);
        Y_ASSERT(it != ChildToKey_.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;
        ChildToKey_.erase(it);
        YCHECK(KeyToChild_.erase(key) == 1);
    }

    virtual void ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild) override
    {
        Y_ASSERT(oldChild);
        Y_ASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToKey_.find(oldChild);
        Y_ASSERT(it != ChildToKey_.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;

        oldChild->SetParent(nullptr);
        ChildToKey_.erase(it);

        KeyToChild_[key] = newChild;
        newChild->SetParent(this);
        YCHECK(ChildToKey_.insert(std::make_pair(newChild, key)).second);
    }

    virtual std::optional<TString> FindChildKey(const IConstNodePtr& child) override
    {
        Y_ASSERT(child);

        auto it = ChildToKey_.find(const_cast<INode*>(child.Get()));
        return it == ChildToKey_.end() ? std::nullopt : std::make_optional(it->second);
    }

private:
    THashMap<TString, INodePtr> KeyToChild_;
    THashMap<INodePtr, TString> ChildToKey_;

    virtual bool DoInvoke(const IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TEphemeralNodeBase::DoInvoke(context);
    }

    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        return TMapNodeMixin::ResolveRecursive(path, context);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase<IListNode>
    , public TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    explicit TListNode(bool shouldHideAttributes)
        : TCompositeNodeBase<IListNode>(shouldHideAttributes)
    { }

    virtual void Clear() override
    {
        for (const auto& node : IndexToChild_) {
            node->SetParent(nullptr);
        }
        IndexToChild_.clear();
        ChildToIndex_.clear();
    }

    virtual int GetChildCount() const override
    {
        return IndexToChild_.size();
    }

    virtual std::vector<INodePtr> GetChildren() const override
    {
        return IndexToChild_;
    }

    virtual INodePtr FindChild(int index) const override
    {
        return index >= 0 && index < IndexToChild_.size() ? IndexToChild_[index] : nullptr;
    }

    virtual void AddChild(const INodePtr& child, int beforeIndex = -1) override
    {
        Y_ASSERT(child);

        if (beforeIndex < 0) {
            YCHECK(ChildToIndex_.insert(std::make_pair(child, static_cast<int>(IndexToChild_.size()))).second);
            IndexToChild_.push_back(child);
        } else {
            YCHECK(beforeIndex <= IndexToChild_.size());
            for (auto it = IndexToChild_.begin() + beforeIndex; it != IndexToChild_.end(); ++it) {
                ++ChildToIndex_[*it];
            }

            YCHECK(ChildToIndex_.insert(std::make_pair(child, beforeIndex)).second);
            IndexToChild_.insert(IndexToChild_.begin() + beforeIndex, child);
        }
        child->SetParent(this);
    }

    virtual bool RemoveChild(int index) override
    {
        if (index < 0 || index >= IndexToChild_.size())
            return false;

        auto child = IndexToChild_[index];

        for (auto it = IndexToChild_.begin() + index + 1; it != IndexToChild_.end(); ++it) {
            --ChildToIndex_[*it];
        }
        IndexToChild_.erase(IndexToChild_.begin() + index);

        YCHECK(ChildToIndex_.erase(child) == 1);
        child->SetParent(nullptr);

        return true;
    }

    virtual void ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild) override
    {
        Y_ASSERT(oldChild);
        Y_ASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToIndex_.find(oldChild);
        Y_ASSERT(it != ChildToIndex_.end());

        int index = it->second;

        oldChild->SetParent(nullptr);

        IndexToChild_[index] = newChild;
        ChildToIndex_.erase(it);
        YCHECK(ChildToIndex_.insert(std::make_pair(newChild, index)).second);
        newChild->SetParent(this);
    }

    virtual void RemoveChild(const INodePtr& child) override
    {
        Y_ASSERT(child);

        int index = GetChildIndexOrThrow(child);
        YCHECK(RemoveChild(index));
    }

    virtual std::optional<int> FindChildIndex(const IConstNodePtr& child) override
    {
        Y_ASSERT(child);

        auto it = ChildToIndex_.find(const_cast<INode*>(child.Get()));
        return it == ChildToIndex_.end() ? std::nullopt : std::make_optional(it->second);
    }

private:
    std::vector<INodePtr> IndexToChild_;
    THashMap<INodePtr, int> ChildToIndex_;

    virtual TResolveResult ResolveRecursive(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        return TListNodeMixin::ResolveRecursive(path, context);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TEphemeralNodeBase
    , public virtual IEntityNode
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    explicit TEntityNode(bool shouldHideAttributes)
        : TEphemeralNodeBase(shouldHideAttributes)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeFactory
    : public TTransactionalNodeFactoryBase
{
public:
    explicit TEphemeralNodeFactory(bool shouldHideAttributes)
        : ShouldHideAttributes_(shouldHideAttributes)
    { }

    virtual ~TEphemeralNodeFactory() override
    {
        RollbackIfNeeded();
    }

    virtual IStringNodePtr CreateString() override
    {
        return New<TStringNode>(ShouldHideAttributes_);
    }

    virtual IInt64NodePtr CreateInt64() override
    {
        return New<TInt64Node>(ShouldHideAttributes_);
    }

    virtual IUint64NodePtr CreateUint64() override
    {
        return New<TUint64Node>(ShouldHideAttributes_);
    }

    virtual IDoubleNodePtr CreateDouble() override
    {
        return New<TDoubleNode>(ShouldHideAttributes_);
    }

    virtual IBooleanNodePtr CreateBoolean() override
    {
        return New<TBooleanNode>(ShouldHideAttributes_);
    }

    virtual IMapNodePtr CreateMap() override
    {
        return New<TMapNode>(ShouldHideAttributes_);
    }

    virtual IListNodePtr CreateList() override
    {
        return New<TListNode>(ShouldHideAttributes_);
    }

    virtual IEntityNodePtr CreateEntity() override
    {
        return New<TEntityNode>(ShouldHideAttributes_);
    }

private:
    const bool ShouldHideAttributes_;

};

std::unique_ptr<ITransactionalNodeFactory> CreateEphemeralNodeFactory(bool shouldHideAttributes)
{
    return std::unique_ptr<ITransactionalNodeFactory>(new TEphemeralNodeFactory(shouldHideAttributes));
}

INodeFactory* GetEphemeralNodeFactory(bool shouldHideAttributes)
{
    static auto hidingFactory = CreateEphemeralNodeFactory(true);
    static auto nonhidingFactory = CreateEphemeralNodeFactory(false);
    return shouldHideAttributes ? hidingFactory.get() : nonhidingFactory.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

