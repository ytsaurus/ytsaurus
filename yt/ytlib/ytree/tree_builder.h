#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp

class TTreeBuilder
    : public IYsonConsumer
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

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes)
    {
        UNUSED(hasAttributes);
        auto node = Factory->CreateString();
        node->SetValue(value);
        Push(~node);
    }

    virtual void OnInt64Scalar(i64 value, bool hasAttributes)
    {
        UNUSED(hasAttributes);
        auto node = Factory->CreateInt64();
        node->SetValue(value);
        Push(~node);
    }

    virtual void OnDoubleScalar(double value, bool hasAttributes)
    {
        UNUSED(hasAttributes);
        auto node = Factory->CreateDouble();
        node->SetValue(value);
        Push(~node);
    }

    virtual void OnEntity(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        Push(~Factory->CreateEntity());
    }


    virtual void OnBeginList()
    {
        Push(~Factory->CreateList());
        Push(NULL);
    }

    virtual void OnListItem()
    {
        AddToList();
    }

    virtual void OnEndList(bool hasAttributes)
    {
        UNUSED(hasAttributes);
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


    virtual void OnBeginMap()
    {
        Push(~Factory->CreateMap());
        Push(NULL);
        Push(NULL);
    }

    virtual void OnMapItem(const Stroka& name)
    {
        AddToMap();
        auto node = Factory->CreateString();
        node->SetValue(name);
        Push(~node);
    }

    virtual void OnEndMap(bool hasAttributes)
    {
        UNUSED(hasAttributes);
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

    
    virtual void OnBeginAttributes()
    {
        OnBeginMap();
    }

    virtual void OnAttributesItem(const Stroka& name)
    {
        OnMapItem(name);
    }

    virtual void OnEndAttributes()
    {
        OnEndMap(false);
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

