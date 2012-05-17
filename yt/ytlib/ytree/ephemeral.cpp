#include "stdafx.h"
#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "attribute_provider_detail.h"

#include <ytlib/misc/hash.h>
#include <ytlib/misc/singleton.h>

#include <algorithm>

namespace NYT {
namespace NYTree {

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

    virtual ICompositeNodePtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNode* parent)
    {
        YASSERT(!parent || !Parent);
        Parent = parent;
    }

protected:
    // TSupportsAttributes members
    virtual IAttributeDictionary* GetUserAttributes()
    {
        return &Attributes();
    }

private:
    ICompositeNode* Parent;
    TAutoPtr<IAttributeDictionary> Attributes_;

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

    virtual yvector< TPair<Stroka, INodePtr> > GetChildren() const
    {
        return yvector< TPair<Stroka, INodePtr> >(KeyToChild.begin(), KeyToChild.end());
    }

    virtual yvector<Stroka> GetKeys() const
    {
        yvector<Stroka> result;
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

    virtual bool AddChild(INode* child, const TStringBuf& key)
    {
        YASSERT(!key.empty());
        YASSERT(child);

        if (KeyToChild.insert(MakePair(key, child)).second) {
            YVERIFY(ChildToKey.insert(MakePair(child, key)).second);
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
        YVERIFY(ChildToKey.erase(child) == 1);

        return true;
    }

    virtual void RemoveChild(INode* child)
    {
        YASSERT(child);

        child->SetParent(NULL);

        auto it = ChildToKey.find(child);
        YASSERT(it != ChildToKey.end());

        // NB: don't use const auto& here, it becomes invalid!
        auto key = it->second;
        ChildToKey.erase(it);
        YVERIFY(KeyToChild.erase(key) == 1);
    }

    virtual void ReplaceChild(INode* oldChild, INode* newChild)
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
        YVERIFY(ChildToKey.insert(MakePair(newChild, key)).second);
    }

    virtual Stroka GetChildKey(const INode* child)
    {
        YASSERT(child);

        auto it = ChildToKey.find(const_cast<INode*>(child));
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

    virtual void SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
    {
        UNUSED(response);

        auto factory = CreateFactory();
        TMapNodeMixin::SetRecursive(~factory, path, request);
        context->Reply();
    }

    virtual void SetNodeRecursive(const TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context)
    {
        UNUSED(response);

        auto value = reinterpret_cast<INode*>(request->value_ptr());
        TMapNodeMixin::SetRecursive(path, value);
        context->Reply();
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
        return IndexToChild.ysize();
    }

    virtual yvector<INodePtr> GetChildren() const
    {
        return IndexToChild;
    }

    virtual INodePtr FindChild(int index) const
    {
        return index >= 0 && index < IndexToChild.ysize() ? IndexToChild[index] : NULL;
    }

    virtual void AddChild(INode* child, int beforeIndex = -1)
    {
        YASSERT(child);

        if (beforeIndex < 0) {
            YVERIFY(ChildToIndex.insert(MakePair(child, IndexToChild.ysize())).second);
            IndexToChild.push_back(child); 
        } else {
            for (auto it = IndexToChild.begin() + beforeIndex; it != IndexToChild.end(); ++it) {
                ++ChildToIndex[*it];
            }

            YVERIFY(ChildToIndex.insert(MakePair(child, beforeIndex)).second);
            IndexToChild.insert(IndexToChild.begin() + beforeIndex, child);
        }
        child->SetParent(this);
    }

    virtual bool RemoveChild(int index)
    {
        if (index < 0 || index >= IndexToChild.ysize())
            return false;

        auto child = IndexToChild[index];

        for (auto it = IndexToChild.begin() + index + 1; it != IndexToChild.end(); ++it) {
            --ChildToIndex[*it];
        }
        IndexToChild.erase(IndexToChild.begin() + index);

        YVERIFY(ChildToIndex.erase(child));
        child->SetParent(NULL);

        return true;
    }

    virtual void ReplaceChild(INode* oldChild, INode* newChild)
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
        YVERIFY(ChildToIndex.insert(MakePair(newChild, index)).second);
        newChild->SetParent(this);
    }

    virtual void RemoveChild(INode* child)
    {
        YASSERT(child);

        int index = GetChildIndex(child);
        YVERIFY(RemoveChild(index));
    }

    virtual int GetChildIndex(const INode* child)
    {
        YASSERT(child);

        auto it = ChildToIndex.find(const_cast<INode*>(child));
        YASSERT(it != ChildToIndex.end());
        return it->second;
    }

private:
    yvector<INodePtr> IndexToChild;
    yhash_map<INodePtr, int> ChildToIndex;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb)
    {
        return TListNodeMixin::ResolveRecursive(path, verb);
    }

    virtual void SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
    {
        UNUSED(response);

        auto factory = CreateFactory();
        TListNodeMixin::SetRecursive(~factory, path, request);
        context->Reply();
    }

    virtual void SetNodeRecursive(const TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context)
    {
        UNUSED(response);

        auto value = reinterpret_cast<INode*>(request->value_ptr());
        TListNodeMixin::SetRecursive(path, value);
        context->Reply();
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

INodeFactory* GetEphemeralNodeFactory()
{
    return ~RefCountedSingleton<TEphemeralNodeFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

