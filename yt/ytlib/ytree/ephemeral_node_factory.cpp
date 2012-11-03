#include "stdafx.h"
#include "ephemeral_node_factory.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ephemeral_attribute_owner.h"
#include "ypath_client.h"

#include <ytlib/misc/hash.h>
#include <ytlib/misc/singleton.h>

#include <algorithm>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TEphemeralYPathResolver
    : public IYPathResolver
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
    INodePtr Node;

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
    TEphemeralNodeBase()
        : Parent(NULL)
    { }


    virtual INodeFactoryPtr CreateFactory() const override
    {
        return GetEphemeralNodeFactory();
    }

    virtual IYPathResolverPtr GetResolver() const override
    {
        return New<TEphemeralYPathResolver>(const_cast<TEphemeralNodeBase*>(this));
    }


    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNodePtr parent) override
    {
        YASSERT(!parent || !Parent);
        Parent = ~parent;
    }


    virtual void SerializeAttributes(
        IYsonConsumer* consumer,
        const TAttributeFilter& filter) const override
    {
        if (filter.Mode == EAttributeFilterMode::None ||
            filter.Mode == EAttributeFilterMode::MatchingOnly && filter.Keys.empty())
            return;

        const auto& attributes = Attributes();
        auto keys = attributes.List();
        yhash_set<Stroka> matchingKeys(filter.Keys.begin(), filter.Keys.end());
        bool seenMatching = false;
        FOREACH (const auto& key, keys) {
            if (filter.Mode == EAttributeFilterMode::All || matchingKeys.find(key) != matchingKeys.end()) {
                if (!seenMatching) {
                    consumer->OnBeginAttributes();
                    seenMatching = true;
                }
                consumer->OnKeyedItem(key);
                consumer->OnRaw(attributes.GetYson(key).Data(), EYsonType::Node);
            }
        }

        if (seenMatching) {
            consumer->OnEndAttributes();
        }
    }

protected:
    // TSupportsAttributes members
    virtual IAttributeDictionary* GetUserAttributes() override
    {
        return &Attributes();
    }

private:
    ICompositeNode* Parent;

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase>
class TScalarNode
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
    TScalarNode()
        : Value()
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
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Integer, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase>
class TCompositeNodeBase
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
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
    virtual void Clear() override
    {
        FOREACH (const auto& pair, KeyToChild) {
            pair.second->SetParent(NULL);
        }
        KeyToChild.clear();
        ChildToKey.clear();
    }

    virtual int GetChildCount() const override
    {
        return KeyToChild.ysize();
    }

    virtual std::vector< TPair<Stroka, INodePtr> > GetChildren() const override
    {
        return std::vector< TPair<Stroka, INodePtr> >(KeyToChild.begin(), KeyToChild.end());
    }

    virtual std::vector<Stroka> GetKeys() const override
    {
        std::vector<Stroka> result;
        result.reserve(KeyToChild.size());
        FOREACH (const auto& pair, KeyToChild) {
            result.push_back(pair.first);
        }
        return result;
    }

    virtual INodePtr FindChild(const Stroka& key) const override
    {
        auto it = KeyToChild.find(key);
        return it == KeyToChild.end() ? NULL : it->second;
    }

    virtual bool AddChild(INodePtr child, const Stroka& key) override
    {
        YASSERT(!key.empty());
        YASSERT(child);

        if (KeyToChild.insert(MakePair(key, child)).second) {
            YCHECK(ChildToKey.insert(MakePair(child, key)).second);
            child->SetParent(this);
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const Stroka& key) override
    {
        auto it = KeyToChild.find(Stroka(key));
        if (it == KeyToChild.end())
            return false;

        auto child = it->second; 
        child->SetParent(NULL);
        KeyToChild.erase(it);
        YCHECK(ChildToKey.erase(child) == 1);

        return true;
    }

    virtual void RemoveChild(INodePtr child) override
    {
        YASSERT(child);

        child->SetParent(NULL);

        auto it = ChildToKey.find(child);
        YASSERT(it != ChildToKey.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;
        ChildToKey.erase(it);
        YCHECK(KeyToChild.erase(key) == 1);
    }

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild) override
    {
        YASSERT(oldChild);
        YASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToKey.find(oldChild);
        YASSERT(it != ChildToKey.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;

        oldChild->SetParent(NULL);
        ChildToKey.erase(it);

        KeyToChild[key] = newChild;
        newChild->SetParent(this);
        YCHECK(ChildToKey.insert(MakePair(newChild, key)).second);
    }

    virtual Stroka GetChildKey(IConstNodePtr child) override
    {
        YASSERT(child);

        auto it = ChildToKey.find(const_cast<INode*>(~child));
        YASSERT(it != ChildToKey.end());
        return it->second;
    }

private:
    yhash_map<Stroka, INodePtr> KeyToChild;
    yhash_map<INodePtr, Stroka> ChildToKey;

    virtual void DoInvoke(IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TEphemeralNodeBase::DoInvoke(context);
    }

    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        IServiceContextPtr context) override
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
    virtual void Clear() override
    {
        FOREACH (const auto& node, IndexToChild) {
            node->SetParent(NULL);
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
        return index >= 0 && index < IndexToChild.size() ? IndexToChild[index] : NULL;
    }

    virtual void AddChild(INodePtr child, int beforeIndex = -1) override
    {
        YASSERT(child);

        if (beforeIndex < 0) {
            YCHECK(ChildToIndex.insert(MakePair(child, IndexToChild.size())).second);
            IndexToChild.push_back(child); 
        } else {
            for (auto it = IndexToChild.begin() + beforeIndex; it != IndexToChild.end(); ++it) {
                ++ChildToIndex[*it];
            }

            YCHECK(ChildToIndex.insert(MakePair(child, beforeIndex)).second);
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
        child->SetParent(NULL);

        return true;
    }

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild) override
    {
        YASSERT(oldChild);
        YASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToIndex.find(oldChild);
        YASSERT(it != ChildToIndex.end());

        int index = it->second;

        oldChild->SetParent(NULL);

        IndexToChild[index] = newChild;
        ChildToIndex.erase(it);
        YCHECK(ChildToIndex.insert(MakePair(newChild, index)).second);
        newChild->SetParent(this);
    }

    virtual void RemoveChild(INodePtr child) override
    {
        YASSERT(child);

        int index = GetChildIndex(child);
        YCHECK(RemoveChild(index));
    }

    virtual int GetChildIndex(IConstNodePtr child) override
    {
        YASSERT(child);

        auto it = ChildToIndex.find(const_cast<INode*>(~child));
        YASSERT(it != ChildToIndex.end());
        return it->second;
    }

private:
    std::vector<INodePtr> IndexToChild;
    yhash_map<INodePtr, int> ChildToIndex;

    virtual TResolveResult ResolveRecursive(
        const TYPath& path,
        IServiceContextPtr context) override
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
};

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeFactory
    : public INodeFactory
{
public:
    virtual IStringNodePtr CreateString() override
    {
        return New<TStringNode>();
    }

    virtual IIntegerNodePtr CreateInteger() override
    {
        return New<TIntegerNode>();
    }

    virtual IDoubleNodePtr CreateDouble() override
    {
        return New<TDoubleNode>();
    }

    virtual IMapNodePtr CreateMap() override
    {
        return New<TMapNode>();
    }

    virtual IListNodePtr CreateList() override
    {
        return New<TListNode>();
    }

    virtual IEntityNodePtr CreateEntity() override
    {
        return New<TEntityNode>();
    }
};

INodeFactoryPtr GetEphemeralNodeFactory()
{
    return RefCountedSingleton<TEphemeralNodeFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

