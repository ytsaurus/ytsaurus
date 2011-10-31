#include "tree_builder.h"
#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TTreeBuilder::TTreeBuilder(INodeFactory* factory)
    : Factory(factory)
{ }

INode::TPtr TTreeBuilder::GetRoot() const
{
    YASSERT(Stack.ysize() == 1);
    return Stack[0];
}

TYsonBuilder::TPtr TTreeBuilder::CreateYsonBuilder(INodeFactory* factory)
{
    return FromFunctor([=] (TYsonProducer::TPtr producer) -> INode::TPtr
        {
            TTreeBuilder builder(factory);
            producer->Do(&builder);
            return builder.GetRoot();
        });
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

INode::TPtr TTreeBuilder::YsonBuilderThunk(
    TYsonProducer::TPtr producer,
    INodeFactory* factory)
{
    TTreeBuilder builder(factory);
    producer->Do(&builder);
    return builder.GetRoot();
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

