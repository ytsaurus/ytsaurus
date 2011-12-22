#include "stdafx.h"
#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"

#include "../misc/hash.h"
#include "../misc/assert.h"
#include "../misc/singleton.h"

#include <algorithm>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeBase
    : public virtual ::NYT::NYTree::TNodeBase
{
public:
    TEphemeralNodeBase();

    virtual INodeFactory::TPtr CreateFactory() const;

    virtual ICompositeNode::TPtr GetParent() const;
    virtual void SetParent(ICompositeNode* parent);

    virtual IMapNode::TPtr GetAttributes() const;
    virtual void SetAttributes(IMapNode* attributes);

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
    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const;
    virtual INode::TPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(INode* child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(INode* oldChild, INode* newChild);
    virtual void RemoveChild(INode* child);
    virtual Stroka GetChildKey(INode* child);

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual IYPathService::TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);
    virtual void SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context);
    virtual void SetNodeRecursive(const TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context);

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase<IListNode>
    , public TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<INode::TPtr> GetChildren() const;
    virtual INode::TPtr FindChild(int index) const;
    virtual void AddChild(INode* child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(INode* oldChild, INode* newChild);
    virtual void RemoveChild(INode* child);
    virtual int GetChildIndex(INode* child);

private:
    yvector<INode::TPtr> IndexToChild;
    yhash_map<INode::TPtr, int> ChildToIndex;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);
    virtual void SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context);
    virtual void SetNodeRecursive(const TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context);

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TEphemeralNodeBase
    , public virtual IEntityNode
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)
};

////////////////////////////////////////////////////////////////////////////////

#undef YTREE_NODE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

TEphemeralNodeBase::TEphemeralNodeBase()
    : Parent(NULL)
{ }

INodeFactory::TPtr TEphemeralNodeBase::CreateFactory() const
{
    return GetEphemeralNodeFactory();
}

ICompositeNode::TPtr TEphemeralNodeBase::GetParent() const
{
    return Parent;
}

void TEphemeralNodeBase::SetParent(ICompositeNode* parent)
{
    YASSERT(!parent || !Parent);
    Parent = parent;
}

IMapNode::TPtr TEphemeralNodeBase::GetAttributes() const
{
    return Attributes;
}

void TEphemeralNodeBase::SetAttributes(IMapNode* attributes)
{
    if (Attributes) {
        Attributes->SetParent(NULL);
        Attributes = NULL;
    }
    Attributes = attributes;
}

////////////////////////////////////////////////////////////////////////////////

void TMapNode::Clear()
{
    FOREACH(const auto& pair, NameToChild) {
        pair.Second()->SetParent(NULL);
    }
    NameToChild.clear();
    ChildToName.clear();
}

int TMapNode::GetChildCount() const
{
    return NameToChild.ysize();
}

yvector< TPair<Stroka, INode::TPtr> > TMapNode::GetChildren() const
{
    return yvector< TPair<Stroka, INode::TPtr> >(NameToChild.begin(), NameToChild.end());
}

INode::TPtr TMapNode::FindChild(const Stroka& name) const
{
    auto it = NameToChild.find(name);
    return it == NameToChild.end() ? NULL : it->Second();
}

bool TMapNode::AddChild(INode* child, const Stroka& name)
{
    YASSERT(!name.empty());

    if (NameToChild.insert(MakePair(name, child)).Second()) {
        YVERIFY(ChildToName.insert(MakePair(child, name)).Second());
        child->SetParent(this);
        return true;
    } else {
        return false;
    }
}

bool TMapNode::RemoveChild(const Stroka& name)
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

void TMapNode::RemoveChild(INode* child)
{
    child->SetParent(NULL);

    auto it = ChildToName.find(child);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();
    ChildToName.erase(it);
    YVERIFY(NameToChild.erase(name) == 1);
}

void TMapNode::ReplaceChild(INode* oldChild, INode* newChild)
{
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

Stroka TMapNode::GetChildKey(INode* child)
{
    auto it = ChildToName.find(child);
    YASSERT(it != ChildToName.end());
    return it->Second();
}

void TMapNode::DoInvoke(NRpc::IServiceContext* context)
{
    if (TMapNodeMixin::DoInvoke(context)) {
        // Do nothing, the verb is already handled.
    } else {
        TEphemeralNodeBase::DoInvoke(context);
    }
}

IYPathService::TResolveResult TMapNode::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TMapNodeMixin::ResolveRecursive(path, verb);
}

void TMapNode::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    TMapNodeMixin::SetRecursive(~factory, path, request);
    context->Reply();
}

void TMapNode::SetNodeRecursive(
    const TYPath& path,
    TReqSetNode* request,
    TRspSetNode* response,
    TCtxSetNode* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    auto value = reinterpret_cast<INode*>(request->value());
    TMapNodeMixin::SetRecursive(~factory, path, value);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

void TListNode::Clear()
{
    FOREACH(const auto& node, IndexToChild) {
        node->SetParent(NULL);
    }
    IndexToChild.clear();
    ChildToIndex.clear();
}

int TListNode::GetChildCount() const
{
    return IndexToChild.ysize();
}

yvector<INode::TPtr> TListNode::GetChildren() const
{
    return IndexToChild;
}

INode::TPtr TListNode::FindChild(int index) const
{
    return index >= 0 && index < IndexToChild.ysize() ? IndexToChild[index] : NULL;
}

void TListNode::AddChild(INode* child, int beforeIndex /*= -1*/)
{
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

bool TListNode::RemoveChild(int index)
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

void TListNode::ReplaceChild(INode* oldChild, INode* newChild)
{
    auto it = ChildToIndex.find(oldChild);
    YASSERT(it != ChildToIndex.end());

    int index = it->Second();

    oldChild->SetParent(NULL);

    IndexToChild[index] = newChild;
    ChildToIndex.erase(it);
    YVERIFY(ChildToIndex.insert(MakePair(newChild, index)).Second());
    newChild->SetParent(this);
}

void TListNode::RemoveChild(INode* child)
{
    int index = GetChildIndex(child);
    YVERIFY(RemoveChild(index));
}

int TListNode::GetChildIndex(INode* child)
{
    auto it = ChildToIndex.find(child);
    YASSERT(it != ChildToIndex.end());
    return it->Second();
}

IYPathService::TResolveResult TListNode::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TListNodeMixin::ResolveRecursive(path, verb);
}

void TListNode::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    TListNodeMixin::SetRecursive(~factory, path, request);
    context->Reply();
}

void TListNode::SetNodeRecursive(
    const TYPath& path,
    TReqSetNode* request,
    TRspSetNode* response,
    TCtxSetNode* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    auto value = reinterpret_cast<INode*>(request->value());
    TListNodeMixin::SetRecursive(~factory, path, value);
    context->Reply();
}

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

