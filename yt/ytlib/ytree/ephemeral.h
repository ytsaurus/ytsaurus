#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath.h"

#include "../misc/hash.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public ::NYT::NYTree::TNodeBase
{
public:
    virtual INodeFactory* GetFactory() const;

};

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
class TScalarNode
    : public TNodeBase
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

#define DECLARE_TYPE_OVERRIDES(name) \
    virtual ENodeType GetType() const \
    { \
        return ENodeType::name; \
    } \
    \
    virtual I ## name ## Node::TConstPtr As ## name() const \
    { \
        return const_cast<T ## name ## Node*>(this); \
    } \
    \
    virtual I ## name ## Node::TPtr As ## name() \
    { \
        return this; \
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
    public: \
        DECLARE_TYPE_OVERRIDES(name) \
    \
    private: \
        \
    };

////////////////////////////////////////////////////////////////////////////////

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TNodeBase
    , public virtual IMapNode
{
public:
    DECLARE_TYPE_OVERRIDES(Map)

    virtual void Clear()
    {
        FOREACH(const auto& pair, NameToChild) {
            pair.Second()->AsMutable()->SetParent(NULL);
        }
        NameToChild.clear();
        ChildToName.clear();
    }

    virtual int GetChildCount() const
    {
        return NameToChild.ysize();
    }

    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const
    {
        return yvector< TPair<Stroka, INode::TConstPtr> >(NameToChild.begin(), NameToChild.end());
    }

    virtual INode::TConstPtr FindChild(const Stroka& name) const
    {
        auto it = NameToChild.find(name);
        return it == NameToChild.end() ? NULL : it->Second();
    }

    virtual bool AddChild(INode::TPtr child, const Stroka& name)
    {
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
        child->AsMutable()->SetParent(NULL);
        NameToChild.erase(it);
        YVERIFY(ChildToName.erase(child) == 1);

        return true;
    }

    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
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

    virtual void RemoveChild(INode::TPtr child)
    {
        auto it = ChildToName.find(child);
        YASSERT(it != ChildToName.end());

        Stroka name = it->Second();
        ChildToName.erase(it);
        YVERIFY(NameToChild.erase(name) == 1);
    }

    virtual TNavigateResult YPathNavigate(
        const TYPath& path) const;

    virtual TSetResult YPathSet(
        const TYPath& path,
        TYsonProducer::TPtr producer);

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

    //void DoAssign(IMapNode::TPtr other)
    //{
    //    // TODO: attributes
    //    Clear();
    //    auto children = other->GetChildren();
    //    other->Clear();
    //    FOREACH(const auto& pair, children) {
    //        AddChild(pair.Second()->AsMutable(), pair.First());
    //    }
    //}

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TNodeBase
    , public virtual IListNode
{
public:
    DECLARE_TYPE_OVERRIDES(List)

    virtual void Clear()
    {
        FOREACH(const auto& node, List) {
            node->AsMutable()->SetParent(NULL);
        }
        List.clear();
    }

    virtual int GetChildCount() const
    {
        return List.ysize();
    }

    virtual yvector<INode::TConstPtr> GetChildren() const
    {
        return List;
    }

    virtual INode::TConstPtr FindChild(int index) const
    {
        return index >= 0 && index < List.ysize() ? List[index] : NULL;
    }

    virtual void AddChild(INode::TPtr child, int beforeIndex = -1)
    {
        if (beforeIndex < 0) {
            List.push_back(child); 
        } else {
            List.insert(List.begin() + beforeIndex, child);
        }
        child->SetParent(this);
    }

    virtual bool RemoveChild(int index)
    {
        if (index < 0 || index >= List.ysize())
            return false;

        List[index]->AsMutable()->SetParent(NULL);
        List.erase(List.begin() + index);
        return true;
    }

    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
    {
        // TODO: implement
        YASSERT(false);
    }

    virtual void RemoveChild(INode::TPtr child)
    {
        // TODO: implement
        YASSERT(false);
    }

private:
    yvector<INode::TConstPtr> List;

    //void YPathSetSelf(IListNode::TPtr other)
    //{
    //    // TODO: attributes
    //    Clear();
    //    auto children = other->GetChildren();
    //    other->Clear();
    //    FOREACH(const auto& child, children) {
    //        AddChild(child->AsMutable());
    //    }
    //}

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TNodeBase
    , public virtual IEntityNode
{
public:
    DECLARE_TYPE_OVERRIDES(Entity)

private:
    void YPathSetSelf(IEntityNode::TPtr other)
    {
        UNUSED(other);
    }
};

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public INodeFactory
{
public:
    static INodeFactory* Get()
    {
        return Singleton<TNodeFactory>();
    }

    virtual IStringNode::TPtr CreateString(const Stroka& value = Stroka())
    {
        IStringNode::TPtr node = ~New<TStringNode>();
        node->SetValue(value);
        return node;
    }

    virtual IInt64Node::TPtr CreateInt64(i64 value = 0)
    {
        IInt64Node::TPtr node = ~New<TInt64Node>();
        node->SetValue(value);
        return node;
    }

    virtual IDoubleNode::TPtr CreateDouble(double value = 0)
    {
        IDoubleNode::TPtr node = ~New<TDoubleNode>();
        node->SetValue(value);
        return node;
    }

    virtual IMapNode::TPtr CreateMap()
    {
        return ~New<TMapNode>();
    }

    virtual IListNode::TPtr CreateList()
    {
        return ~New<TListNode>();
    }

    virtual IEntityNode::TPtr CreateEntity()
    {
        return ~New<TEntityNode>();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT

