#include "stdafx.h"
#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "attribute_provider_detail.h"
#include "ypath_client.h"

#include <ytlib/misc/hash.h>
#include <ytlib/misc/singleton.h>

#include <algorithm>

namespace NYT {
namespace NYTree {

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
    , public TEphemeralAttributeProvider
{
public:
    TEphemeralNodeBase()
        : Parent(NULL)
    { }

    virtual INodeFactoryPtr CreateFactory() const
    {
        return GetEphemeralNodeFactory();
    }

    virtual IYPathResolverPtr GetResolver() const
    {
        return New<TEphemeralYPathResolver>(const_cast<TEphemeralNodeBase*>(this));
    }

    virtual ICompositeNodePtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNodePtr parent)
    {
        YASSERT(!parent || !Parent);
        Parent = ~parent;
    }

protected:
    // TSupportsAttributes members
    virtual IAttributeDictionary* GetUserAttributes()
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

    virtual TValue GetValue() const
    {
        return Value;
    }

    virtual void SetValue(const TValue& value)
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
    virtual TIntrusivePtr<ICompositeNode> AsComposite()
    {
        return this;
    }

    virtual TIntrusivePtr<const ICompositeNode> AsComposite() const
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
    virtual void Clear()
    {
        FOREACH (const auto& pair, KeyToChild) {
            pair.second->SetParent(NULL);
        }
        KeyToChild.clear();
        ChildToKey.clear();
    }

    virtual int GetChildCount() const
    {
        return KeyToChild.ysize();
    }

    virtual std::vector< TPair<Stroka, INodePtr> > GetChildren() const
    {
        return std::vector< TPair<Stroka, INodePtr> >(KeyToChild.begin(), KeyToChild.end());
    }

    virtual std::vector<Stroka> GetKeys() const
    {
        std::vector<Stroka> result;
        result.reserve(KeyToChild.size());
        FOREACH (const auto& pair, KeyToChild) {
            result.push_back(pair.first);
        }
        return result;
    }

    virtual INodePtr FindChild(const TStringBuf& key) const
    {
        auto it = KeyToChild.find(Stroka(key));
        return it == KeyToChild.end() ? NULL : it->second;
    }

    virtual bool AddChild(INodePtr child, const TStringBuf& key)
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

    virtual bool RemoveChild(const TStringBuf& key)
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

    virtual void RemoveChild(INodePtr child)
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

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild)
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

    virtual Stroka GetChildKey(IConstNodePtr child)
    {
        YASSERT(child);

        auto it = ChildToKey.find(const_cast<INode*>(~child));
        YASSERT(it != ChildToKey.end());
        return it->second;
    }

private:
    yhash_map<Stroka, INodePtr> KeyToChild;
    yhash_map<INodePtr, Stroka> ChildToKey;

    virtual void DoInvoke(NRpc::IServiceContextPtr context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TEphemeralNodeBase::DoInvoke(context);
    }

    virtual IYPathService::TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb)
    {
        return TMapNodeMixin::ResolveRecursive(path, verb);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase<IListNode>
    , public TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    virtual void Clear()
    {
        FOREACH (const auto& node, IndexToChild) {
            node->SetParent(NULL);
        }
        IndexToChild.clear();
        ChildToIndex.clear();
    }

    virtual int GetChildCount() const
    {
        return IndexToChild.size();
    }

    virtual std::vector<INodePtr> GetChildren() const
    {
        return IndexToChild;
    }

    virtual INodePtr FindChild(int index) const
    {
        return index >= 0 && index < IndexToChild.size() ? IndexToChild[index] : NULL;
    }

    virtual void AddChild(INodePtr child, int beforeIndex = -1)
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

    virtual bool RemoveChild(int index)
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

    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild)
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

    virtual void RemoveChild(INodePtr child)
    {
        YASSERT(child);

        int index = GetChildIndex(child);
        YVERIFY(RemoveChild(index));
    }

    virtual int GetChildIndex(IConstNodePtr child)
    {
        YASSERT(child);

        auto it = ChildToIndex.find(const_cast<INode*>(~child));
        YASSERT(it != ChildToIndex.end());
        return it->second;
    }

private:
    std::vector<INodePtr> IndexToChild;
    yhash_map<INodePtr, int> ChildToIndex;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb)
    {
        return TListNodeMixin::ResolveRecursive(path, verb);
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
    virtual IStringNodePtr CreateString()
    {
        return New<TStringNode>();
    }

    virtual IIntegerNodePtr CreateInteger()
    {
        return New<TIntegerNode>();
    }

    virtual IDoubleNodePtr CreateDouble()
    {
        return New<TDoubleNode>();
    }

    virtual IMapNodePtr CreateMap()
    {
        return New<TMapNode>();
    }

    virtual IListNodePtr CreateList()
    {
        return New<TListNode>();
    }

    virtual IEntityNodePtr CreateEntity()
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

