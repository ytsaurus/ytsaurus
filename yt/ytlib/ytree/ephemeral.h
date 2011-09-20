#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath.h"

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
/*
    virtual void YPathAssign(INode::TPtr other) \
    { \
        if (GetType() != other->GetType()) { \
            ythrow yexception() << Sprintf("Cannot change node type from %s to %s during update", \
                ~GetType().ToString().Quote(), \
                ~other->GetType().ToString().Quote()); \
        } \
        DoAssign(other->As ## name()); \
    }
*/
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

        //void DoAssign(I ## name ## Node::TPtr other) \
        //{ \
        //    SetValue(other->GetValue()); \
        //} \

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
        FOREACH(const auto& pair, Map) {
            pair.Second()->AsMutable()->SetParent(NULL);
        }
        Map.clear();
    }

    virtual int GetChildCount() const
    {
        return Map.ysize();
    }

    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const
    {
        return yvector< TPair<Stroka, INode::TConstPtr> >(Map.begin(), Map.end());
    }

    virtual INode::TConstPtr FindChild(const Stroka& name) const
    {
        auto it = Map.find(name);
        return it == Map.end() ? NULL : it->Second();
    }

    virtual bool AddChild(INode::TPtr node, const Stroka& name)
    {
        if (Map.insert(MakePair(name, node)).Second()) {
            node->SetParent(TNodeBase::AsImmutable());
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const Stroka& name)
    {
        auto it = Map.find(name);
        if (it == Map.end())
            return false;

        it->Second()->AsMutable()->SetParent(NULL);
        Map.erase(it);
        return true;
    }

    virtual TNavigateResult YPathNavigate(
        const TYPath& path) const;

    virtual TSetResult YPathSet(
        const TYPath& path,
        INode::TConstPtr value)
    {
        return TSetResult::CreateError("");
    }

    //virtual void YPathForce(
    //    const TYPath& path,
    //    INode::TPtr* tailNode)
    //{
    //    YASSERT(!path.empty());

    //    TYPath currentPath = path;
    //    IMapNode::TPtr currentNode = this;
    //    while (!currentPath.empty()) {
    //        TYPath tailPath;
    //        Stroka name;
    //        ChopYPathPrefix(currentPath, &name, &tailPath);
    //        auto child = GetFactory()->CreateMap();
    //        currentNode->AddChild(child->AsNode(), name);
    //        currentNode = child;
    //        currentPath = tailPath;
    //    }

    //    *tailNode = currentNode->AsNode();
    //}

private:
    yhash_map<Stroka, INode::TConstPtr> Map;

    void DoAssign(IMapNode::TPtr other)
    {
        // TODO: attributes
        Clear();
        auto children = other->GetChildren();
        other->Clear();
        FOREACH(const auto& pair, children) {
            AddChild(pair.Second()->AsMutable(), pair.First());
        }
    }

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

    virtual void AddChild(INode::TPtr node, int beforeIndex = -1)
    {
        if (beforeIndex < 0) {
            List.push_back(node); 
        } else {
            List.insert(List.begin() + beforeIndex, node);
        }
        node->SetParent(TNodeBase::AsImmutable());
    }

    virtual bool RemoveChild(int index)
    {
        if (index < 0 || index >= List.ysize())
            return false;

        List[index]->AsMutable()->SetParent(NULL);
        List.erase(List.begin() + index);
        return true;
    }

    //virtual bool YPathNavigate(
    //    const TYPath& path,
    //    INode::TConstPtr* node,
    //    TYPath* tailPath) const
    //{
    //    if (TNodeBase::YPathNavigate(path, node, tailPath))
    //        return true;

    //    Stroka token;
    //    ChopYPathPrefix(path, &token, tailPath);

    //    int index = FromString<int>(token);
    //    auto child = FindChild(index);
    //    if (~child != NULL) {
    //        *node = child;
    //        return true;
    //    } else {
    //        *node = NULL;
    //        *tailPath = path;
    //        return false;
    //    }
    //}

private:
    yvector<INode::TConstPtr> List;

    void DoAssign(IListNode::TPtr other)
    {
        // TODO: attributes
        Clear();
        auto children = other->GetChildren();
        other->Clear();
        FOREACH(const auto& child, children) {
            AddChild(child->AsMutable());
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TNodeBase
    , public virtual IEntityNode
{
public:
    DECLARE_TYPE_OVERRIDES(Entity)

private:
    void DoAssign(IEntityNode::TPtr other)
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

