#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilder
    : public IYsonEvents
{
public:
    TTreeBuilder(INodeFactory* factory)
        : Factory(factory)
    { }

    INode::TPtr GetRoot() const
    {
        YASSERT(Stack.ysize() == 1);
        return Stack[0];
    }

private:
    INodeFactory* Factory;
    yvector<INode::TPtr> Stack;

    virtual void BeginTree()
    {
        YASSERT(Stack.ysize() == 0);
    }

    virtual void EndTree()
    {
        YASSERT(Stack.ysize() == 1);
    }


    virtual void StringValue(const Stroka& value)
    {
        Push(~Factory->CreateString(value));
    }

    virtual void Int64Value(i64 value)
    {
        Push(~Factory->CreateInt64(value));
    }

    virtual void DoubleValue(double value)
    {
        Push(~Factory->CreateDouble(value));
    }

    virtual void EntityValue()
    {
        Push(~Factory->CreateEntity());
    }


    virtual void BeginList()
    {
        Push(~Factory->CreateList());
        Push(NULL);
    }

    virtual void ListItem(int index)
    {
        UNUSED(index);
        AddToList();
    }

    virtual void EndList()
    {
        AddToList();
    }

    void AddToList()
    {
        auto child = Pop();
        auto list = Peek()->AsList();
        if (~child != NULL) {
            list->AddChild(child);
        }
    }


    virtual void BeginMap()
    {
        Push(~Factory->CreateMap());
        Push(NULL);
        Push(NULL);
    }

    virtual void MapItem(const Stroka& name)
    {
        AddToMap();
        Push(~Factory->CreateString(name));
    }

    virtual void EndMap()
    {
        AddToMap();
    }

    void AddToMap()
    {
        auto child = Pop();
        auto name = Pop();
        auto map = Peek()->AsMap();
        if (~child != NULL) {
            map->AddChild(child, name->GetValue<Stroka>());
        }
    }

    
    virtual void BeginAttributes()
    {
        BeginMap();
    }

    virtual void AttributesItem(const Stroka& name)
    {
        MapItem(name);
    }

    virtual void EndAttributes()
    {
        EndMap();
        auto attributes = Pop()->AsMap();
        auto node = Peek();
        node->SetAttributes(attributes);
    }


    void Push(INode::TPtr node)
    {
        Stack.push_back(node);
    }

    INode::TPtr Pop()
    {
        YASSERT(!Stack.empty());
        auto result = Stack.back();
        Stack.pop_back();
        return result;
    }

    INode::TPtr Peek()
    {
        YASSERT(!Stack.empty());
        return Stack.back();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

