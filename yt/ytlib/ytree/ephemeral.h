#pragma once

#include "common.h"
#include "ytree.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
class TScalarNode
    : public TNodeBase<IBase>
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
    };

////////////////////////////////////////////////////////////////////////////////

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TNodeBase<IMapNode>
{
public:
    DECLARE_TYPE_OVERRIDES(Map)

    virtual int GetChildCount() const
    {
        return Map.ysize();
    }

    virtual yvector<Stroka> GetChildNames() const
    {
        yvector<Stroka> result;
        FOREACH(const auto& pair, Map) {
            result.push_back(pair.First());
        }
        return result;
    }

    virtual INode::TConstPtr FindChild(const Stroka& name) const
    {
        auto it = Map.find(name);
        return it == Map.end() ? NULL : it->Second();
    }

    virtual bool AddChild(INode::TPtr node, const Stroka& name)
    {
        if (Map.insert(MakePair(name, node)).Second()) {
            node->SetParent(this);
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

private:
    yhash_map<Stroka, INode::TConstPtr> Map;

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TNodeBase<IListNode>
{
public:
    DECLARE_TYPE_OVERRIDES(List)

    virtual int GetChildCount() const
    {
        return List.ysize();
    }

    virtual INode::TConstPtr FindChild(int index) const
    {
        return index >= 0 && index < List.ysize() ? List[index] : NULL;
    }

    virtual void AddChild(INode::TPtr node, int beforeIndex)
    {
        if (beforeIndex < 0) {
            List.push_back(node); 
        } else {
            List.insert(List.begin() + beforeIndex, node);
        }
        node->SetParent(this);
    }

    virtual bool RemoveChild(int index)
    {
        if (index < 0 || index >= List.ysize())
            return false;

        List[index]->AsMutable()->SetParent(NULL);
        List.erase(List.begin() + index);
        return true;
    }

private:
    yvector<INode::TConstPtr> List;

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TNodeBase<IEntityNode>
{
public:
    DECLARE_TYPE_OVERRIDES(Entity)
};

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : INodeFactory
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

