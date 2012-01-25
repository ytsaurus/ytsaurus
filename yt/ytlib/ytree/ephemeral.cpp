#include "stdafx.h"
#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"

#include <ytlib/misc/hash.h>
#include <ytlib/misc/singleton.h>

#include <algorithm>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeBase
    : public TNodeBase
{
public:
    TEphemeralNodeBase()
        : Parent(NULL)
    { }

    virtual INodeFactory::TPtr CreateFactory() const
    {
        return GetEphemeralNodeFactory();
    }

    virtual ICompositeNode::TPtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNode* parent)
    {
        YASSERT(!parent || !Parent);
        Parent = parent;
    }

protected:
    IMapNode* GetOrCreateAttributes()
    {
        if (!Attributes) {
            Attributes = GetEphemeralNodeFactory()->CreateMap();
        }
        return ~Attributes;
    }

    virtual TResolveResult ResolveAttributes(const TYPath& path, const Stroka& verb)
    {
        auto attributes = GetOrCreateAttributes();
        return TResolveResult::There(attributes, path);
    }

private:
    ICompositeNode* Parent;
    IMapNode::TPtr Attributes;

};

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
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
DECLARE_SCALAR_TYPE(Int64, i64)
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
        FOREACH(const auto& pair, NameToChild) {
            pair.Second()->SetParent(NULL);
        }
        NameToChild.clear();
        ChildToName.clear();
    }

    virtual int GetChildCount() const
    {
        return NameToChild.ysize();
    }

    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const
    {
        return yvector< TPair<Stroka, INode::TPtr> >(NameToChild.begin(), NameToChild.end());
    }

    virtual INode::TPtr FindChild(const Stroka& name) const
    {
        auto it = NameToChild.find(name);
        return it == NameToChild.end() ? NULL : it->Second();
    }

    virtual bool AddChild(INode* child, const Stroka& name)
    {
        YASSERT(!name.empty());
        YASSERT(child);

        if (NameToChild.insert(MakePair(name, child)).Second()) {
            YVERIFY(ChildToName.insert(MakePair(child, name)).Second());
            child->SetParent(this);
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const Stroka& name)
    {
        auto it = NameToChild.find(name);
        if (it == NameToChild.end())
            return false;

        auto child = it->Second(); 
        child->SetParent(NULL);
        NameToChild.erase(it);
        YVERIFY(ChildToName.erase(child) == 1);

        return true;
    }

    virtual void RemoveChild(INode* child)
    {
        YASSERT(child);

        child->SetParent(NULL);

        auto it = ChildToName.find(child);
        YASSERT(it != ChildToName.end());

        Stroka name = it->Second();
        ChildToName.erase(it);
        YVERIFY(NameToChild.erase(name) == 1);
    }

    virtual void ReplaceChild(INode* oldChild, INode* newChild)
    {
        YASSERT(oldChild);
        YASSERT(newChild);

        if (oldChild == newChild)
            return;

        auto it = ChildToName.find(oldChild);
        YASSERT(it != ChildToName.end());

        Stroka name = it->Second();

        oldChild->SetParent(NULL);
        ChildToName.erase(it);

        NameToChild[name] = newChild;
        newChild->SetParent(this);
        YVERIFY(ChildToName.insert(MakePair(newChild, name)).Second());
    }

    virtual Stroka GetChildKey(INode* child)
    {
        YASSERT(child);

        auto it = ChildToName.find(child);
        YASSERT(it != ChildToName.end());
        return it->Second();
    }

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

    virtual void DoInvoke(NRpc::IServiceContext* context)
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

        auto factory = CreateFactory();
        auto value = reinterpret_cast<INode*>(request->value());
        TMapNodeMixin::SetRecursive(~factory, path, value);
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
        FOREACH(const auto& node, IndexToChild) {
            node->SetParent(NULL);
        }
        IndexToChild.clear();
        ChildToIndex.clear();
    }

    virtual int GetChildCount() const
    {
        return IndexToChild.ysize();
    }

    virtual yvector<INode::TPtr> GetChildren() const
    {
        return IndexToChild;
    }

    virtual INode::TPtr FindChild(int index) const
    {
        return index >= 0 && index < IndexToChild.ysize() ? IndexToChild[index] : NULL;
    }

    virtual void AddChild(INode* child, int beforeIndex = -1)
    {
        YASSERT(child);

        if (beforeIndex < 0) {
            YVERIFY(ChildToIndex.insert(MakePair(child, IndexToChild.ysize())).Second());
            IndexToChild.push_back(child); 
        } else {
            for (auto it = IndexToChild.begin() + beforeIndex; it != IndexToChild.end(); ++it) {
                ++ChildToIndex[*it];
            }

            YVERIFY(ChildToIndex.insert(MakePair(child, beforeIndex)).Second());
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

        int index = it->Second();

        oldChild->SetParent(NULL);

        IndexToChild[index] = newChild;
        ChildToIndex.erase(it);
        YVERIFY(ChildToIndex.insert(MakePair(newChild, index)).Second());
        newChild->SetParent(this);
    }

    virtual void RemoveChild(INode* child)
    {
        YASSERT(child);

        int index = GetChildIndex(child);
        YVERIFY(RemoveChild(index));
    }

    virtual int GetChildIndex(INode* child)
    {
        YASSERT(child);

        auto it = ChildToIndex.find(child);
        YASSERT(it != ChildToIndex.end());
        return it->Second();
    }

private:
    yvector<INode::TPtr> IndexToChild;
    yhash_map<INode::TPtr, int> ChildToIndex;

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

        auto factory = CreateFactory();
        auto value = reinterpret_cast<INode*>(request->value());
        TListNodeMixin::SetRecursive(~factory, path, value);
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
    virtual IStringNode::TPtr CreateString()
    {
        return New<TStringNode>();
    }

    virtual IInt64Node::TPtr CreateInt64()
    {
        return New<TInt64Node>();
    }

    virtual IDoubleNode::TPtr CreateDouble()
    {
        return New<TDoubleNode>();
    }

    virtual IMapNode::TPtr CreateMap()
    {
        return New<TMapNode>();
    }

    virtual IListNode::TPtr CreateList()
    {
        return New<TListNode>();
    }

    virtual IEntityNode::TPtr CreateEntity()
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

