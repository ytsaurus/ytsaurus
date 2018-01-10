#include "ephemeral_node_factory.h"
#include "ephemeral_attribute_owner.h"
#include "node_detail.h"
#include "ypath_client.h"
#include "ypath_detail.h"

#include <yt/core/misc/hash.h>
#include <yt/core/misc/singleton.h>

#include <yt/core/yson/async_consumer.h>

#include <algorithm>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TEphemeralYPathResolver
    : public INodeResolver
{
public:
    explicit TEphemeralYPathResolver(INodePtr node)
        : Node(node)
    { }

    virtual INodePtr ResolvePath(const TYPath& path) override
    {
        auto root = GetRoot();
        return GetNodeByYPath(root, path);
    }

    virtual TYPath GetPath(INodePtr node) override
    {
        return GetNodeYPath(node);
    }

private:
    const INodePtr Node;

    INodePtr GetRoot()
    {
        auto currentNode = Node;
        while (currentNode->GetParent()) {
            currentNode = currentNode->GetParent();
        }
        return currentNode;
    }
};

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

    virtual INodeResolverPtr GetResolver() const override
    {
        return New<TEphemeralYPathResolver>(const_cast<TEphemeralNodeBase*>(this));
    }

    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent.Lock();
    }

    virtual void SetParent(ICompositeNodePtr parent) override
    {
        Y_ASSERT(!parent || Parent.IsExpired());
        Parent = parent;
    }

    virtual bool ShouldHideAttributes() override
    {
        return ShouldHideAttributes_;
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const TNullable<std::vector<TString>>& attributeKeys,
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
    TWeakPtr<ICompositeNode> Parent;

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
        , Value()
    { }

    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const override
    {
        return Value;
    }

    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) override
    {
        Value = value;
    }

private:
    TValue Value;

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
        for (const auto& pair : KeyToChild) {
            pair.second->SetParent(nullptr);
        }
        KeyToChild.clear();
        ChildToKey.clear();
    }

    virtual int GetChildCount() const override
    {
        return KeyToChild.ysize();
    }

    virtual std::vector< std::pair<TString, INodePtr> > GetChildren() const override
    {
        return std::vector< std::pair<TString, INodePtr> >(KeyToChild.begin(), KeyToChild.end());
    }

    virtual std::vector<TString> GetKeys() const override
    {
        std::vector<TString> result;
        result.reserve(KeyToChild.size());
        for (const auto& pair : KeyToChild) {
            result.push_back(pair.first);
        }
        return result;
    }

    virtual INodePtr FindChild(const TString& key) const override
    {
        auto it = KeyToChild.find(key);
        return it == KeyToChild.end() ? nullptr : it->second;
    }

    virtual bool AddChild(INodePtr child, const TString& key) override
    {
        Y_ASSERT(child);
        ValidateYTreeKey(key);

        if (KeyToChild.insert(std::make_pair(key, child)).second) {
            YCHECK(ChildToKey.insert(std::make_pair(child, key)).second);
            child->SetParent(this);
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const TString& key) override
    {
        auto it = KeyToChild.find(TString(key));
        if (it == KeyToChild.end())
            return false;

        auto child = it->second;
        child->SetParent(nullptr);
        KeyToChild.erase(it);
        YCHECK(ChildToKey.erase(child) == 1);

        return true;
    }

    virtual void RemoveChild(INodePtr child) override
    {
        Y_ASSERT(child);

        child->SetParent(nullptr);

        auto it = ChildToKey.find(child);
        Y_ASSERT(it != ChildToKey.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;
        ChildToKey.erase(it);
        YCHECK(KeyToChild.erase(key) == 1);
    }

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild) override
    {
        Y_ASSERT(oldChild);
        Y_ASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToKey.find(oldChild);
        Y_ASSERT(it != ChildToKey.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;

        oldChild->SetParent(nullptr);
        ChildToKey.erase(it);

        KeyToChild[key] = newChild;
        newChild->SetParent(this);
        YCHECK(ChildToKey.insert(std::make_pair(newChild, key)).second);
    }

    virtual TString GetChildKey(IConstNodePtr child) override
    {
        Y_ASSERT(child);

        auto it = ChildToKey.find(const_cast<INode*>(child.Get()));
        Y_ASSERT(it != ChildToKey.end());
        return it->second;
    }

private:
    THashMap<TString, INodePtr> KeyToChild;
    THashMap<INodePtr, TString> ChildToKey;

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
    TListNode(bool shouldHideAttributes)
        : TCompositeNodeBase<IListNode>(shouldHideAttributes)
    { }

    virtual void Clear() override
    {
        for (const auto& node : IndexToChild) {
            node->SetParent(nullptr);
        }
        IndexToChild.clear();
        ChildToIndex.clear();
    }

    virtual int GetChildCount() const override
    {
        return IndexToChild.size();
    }

    virtual std::vector<INodePtr> GetChildren() const override
    {
        return IndexToChild;
    }

    virtual INodePtr FindChild(int index) const override
    {
        return index >= 0 && index < IndexToChild.size() ? IndexToChild[index] : nullptr;
    }

    virtual void AddChild(INodePtr child, int beforeIndex = -1) override
    {
        Y_ASSERT(child);

        if (beforeIndex < 0) {
            YCHECK(ChildToIndex.insert(std::make_pair(child, static_cast<int>(IndexToChild.size()))).second);
            IndexToChild.push_back(child);
        } else {
            for (auto it = IndexToChild.begin() + beforeIndex; it != IndexToChild.end(); ++it) {
                ++ChildToIndex[*it];
            }

            YCHECK(ChildToIndex.insert(std::make_pair(child, beforeIndex)).second);
            IndexToChild.insert(IndexToChild.begin() + beforeIndex, child);
        }
        child->SetParent(this);
    }

    virtual bool RemoveChild(int index) override
    {
        if (index < 0 || index >= IndexToChild.size())
            return false;

        auto child = IndexToChild[index];

        for (auto it = IndexToChild.begin() + index + 1; it != IndexToChild.end(); ++it) {
            --ChildToIndex[*it];
        }
        IndexToChild.erase(IndexToChild.begin() + index);

        YCHECK(ChildToIndex.erase(child) == 1);
        child->SetParent(nullptr);

        return true;
    }

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild) override
    {
        Y_ASSERT(oldChild);
        Y_ASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToIndex.find(oldChild);
        Y_ASSERT(it != ChildToIndex.end());

        int index = it->second;

        oldChild->SetParent(nullptr);

        IndexToChild[index] = newChild;
        ChildToIndex.erase(it);
        YCHECK(ChildToIndex.insert(std::make_pair(newChild, index)).second);
        newChild->SetParent(this);
    }

    virtual void RemoveChild(INodePtr child) override
    {
        Y_ASSERT(child);

        int index = GetChildIndex(child);
        YCHECK(RemoveChild(index));
    }

    virtual int GetChildIndex(IConstNodePtr child) override
    {
        Y_ASSERT(child);

        auto it = ChildToIndex.find(const_cast<INode*>(child.Get()));
        Y_ASSERT(it != ChildToIndex.end());
        return it->second;
    }

private:
    std::vector<INodePtr> IndexToChild;
    THashMap<INodePtr, int> ChildToIndex;

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

} // namespace NYTree
} // namespace NYT

