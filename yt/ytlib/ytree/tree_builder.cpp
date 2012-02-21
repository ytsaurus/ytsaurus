#include "stdafx.h"
#include "tree_builder.h"
#include "ypath_client.h"
#include "serialize.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/misc/assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilder
    : public ITreeBuilder
{
public:
    TTreeBuilder(INodeFactory* factory);

    virtual void BeginTree();
    virtual INodePtr EndTree();

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
    //! Contains nodes forming the current path in the tree.
    yvector<INodePtr> NodeStack;
    //! Contains names of the currently active map children.
    yvector<Stroka> NameStack;

    void AddToList();
    void AddToMap();

    void PushName(const Stroka& name);
    Stroka PopName();

    void PushNode(INode* node);
    INodePtr PopPop();
    INodePtr PeekPop();
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
    NodeStack.clear();
    NameStack.clear();
}

INodePtr TTreeBuilder::EndTree()
{
    // Failure here means that the tree is not fully constructed yet.
    YASSERT(NodeStack.ysize() == 1);
    YASSERT(NameStack.ysize() == 0);

    auto node = NodeStack[0];
    NodeStack.clear();
    return node;
}

void TTreeBuilder::OnNode(INode* node)
{
    PushNode(node);
}

void TTreeBuilder::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateString();
    node->SetValue(value);
    PushNode(~node);
}

void TTreeBuilder::OnInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateInt64();
    node->SetValue(value);
    PushNode(~node);
}

void TTreeBuilder::OnDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    auto node = Factory->CreateDouble();
    node->SetValue(value);
    PushNode(~node);
}

void TTreeBuilder::OnEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    PushNode(~Factory->CreateEntity());
}

void TTreeBuilder::OnBeginList()
{
    PushNode(~Factory->CreateList());
    PushNode(NULL);
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
    PushNode(~Factory->CreateMap());
    PushName("");
    PushNode(NULL);
}

void TTreeBuilder::OnMapItem(const Stroka& name)
{
    AddToMap();
    PushName(name);
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
    auto attributes = PopPop()->AsMap();
    auto attributesYson = SerializeToYson(~attributes);
    auto node = PeekPop();
    SyncYPathSet(~node, AttributeMarker, attributesYson);
}

void TTreeBuilder::AddToList()
{
    auto child = PopPop();
    auto list = PeekPop()->AsList();
    if (child) {
        list->AddChild(~child);
    }
}

void TTreeBuilder::AddToMap()
{
    auto child = PopPop();
    auto name = PopName();
    auto map = PeekPop()->AsMap();
    if (child) {
        YVERIFY(map->AddChild(~child, name));
    }
}

void TTreeBuilder::PushNode(INode* node)
{
    NodeStack.push_back(node);
}

INodePtr TTreeBuilder::PopPop()
{
    YASSERT(!NodeStack.empty());
    auto result = NodeStack.back();
    NodeStack.pop_back();
    return result;
}

INodePtr TTreeBuilder::PeekPop()
{
    YASSERT(!NodeStack.empty());
    return NodeStack.back();
}

void TTreeBuilder::PushName(const Stroka& name)
{
    NameStack.push_back(name);
}

Stroka TTreeBuilder::PopName()
{
    YASSERT(!NameStack.empty());
    auto result = NameStack.back();
    NameStack.pop_back();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

