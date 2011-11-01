#include "stdafx.h"
#include "tree_builder.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilder
    : public ITreeBuilder
{
public:
    TTreeBuilder(INodeFactory* factory);

    virtual void BeginTree();
    virtual INode::TPtr EndTree();

    virtual void OnNode(INode* node);

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList(bool hasAttributes);

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap(bool hasAttributes);

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

private:
    INodeFactory* Factory;
    yvector<INode::TPtr> Stack;

    void AddToList();
    void AddToMap();

    void Push(INode::TPtr node);
    INode::TPtr Pop();
    INode::TPtr Peek();
};

TAutoPtr<ITreeBuilder> CreateBuilderFromFactory(INodeFactory* factory)
{
    return new TTreeBuilder(factory);

}

////////////////////////////////////////////////////////////////////////////////

TTreeBuilder::TTreeBuilder(INodeFactory* factory)
    : Factory(factory)
{ }

void TTreeBuilder::BeginTree()
{
    Stack.clear();
}

INode::TPtr TTreeBuilder::EndTree()
{
    YASSERT(Stack.ysize() == 1);
    auto node = Stack[0];
    Stack.clear();
    return node;
}

void TTreeBuilder::OnNode(INode* node)
{
    Push(node);
}

void TTreeBuilder::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateString();
    node->SetValue(value);
    Push(~node);
}

void TTreeBuilder::OnInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateInt64();
    node->SetValue(value);
    Push(~node);
}

void TTreeBuilder::OnDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateDouble();
    node->SetValue(value);
    Push(~node);
}

void TTreeBuilder::OnEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    Push(~Factory->CreateEntity());
}

void TTreeBuilder::OnBeginList()
{
    Push(~Factory->CreateList());
    Push(NULL);
}

void TTreeBuilder::OnListItem()
{
    AddToList();
}

void TTreeBuilder::OnEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    AddToList();
}

void TTreeBuilder::OnBeginMap()
{
    Push(~Factory->CreateMap());
    Push(NULL);
    Push(NULL);
}

void TTreeBuilder::OnMapItem(const Stroka& name)
{
    AddToMap();
    auto node = Factory->CreateString();
    node->SetValue(name);
    Push(~node);
}

void TTreeBuilder::OnEndMap(bool hasAttributes)
{
    UNUSED(hasAttributes);
    AddToMap();
}

void TTreeBuilder::OnBeginAttributes()
{
    OnBeginMap();
}

void TTreeBuilder::OnAttributesItem(const Stroka& name)
{
    OnMapItem(name);
}

void TTreeBuilder::OnEndAttributes()
{
    OnEndMap(false);
    auto attributes = Pop()->AsMap();
    auto node = Peek();
    node->SetAttributes(attributes);
}

void TTreeBuilder::AddToList()
{
    auto child = Pop();
    auto list = Peek()->AsList();
    if (~child != NULL) {
        list->AddChild(child);
    }
}

void TTreeBuilder::AddToMap()
{
    auto child = Pop();
    auto name = Pop();
    auto map = Peek()->AsMap();
    if (~child != NULL) {
        map->AddChild(child, name->GetValue<Stroka>());
    }
}

void TTreeBuilder::Push(INode::TPtr node)
{
    Stack.push_back(node);
}

INode::TPtr TTreeBuilder::Pop()
{
    YASSERT(!Stack.empty());
    auto result = Stack.back();
    Stack.pop_back();
    return result;
}

INode::TPtr TTreeBuilder::Peek()
{
    YASSERT(!Stack.empty());
    return Stack.back();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

