#include "stdafx.h"
#include "tree_builder.h"

#include "attributes.h"
#include "forwarding_yson_consumer.h"
#include "attribute_consumer.h"

#include <ytlib/actions/bind.h>
#include <ytlib/misc/assert.h>

#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilder
    : public TForwardingYsonConsumer
    , public ITreeBuilder
{
public:
    TTreeBuilder(INodeFactory* factory)
        : Factory(factory)
    { }

    virtual void BeginTree()
    {
        YASSERT(NodeStack.size() == 0);
    }

    virtual INodePtr EndTree()
    {
        // Failure here means that the tree is not fully constructed yet.
        YASSERT(NodeStack.size() == 0);
        YASSERT(ResultNode);

        return ResultNode;
    }

    virtual void OnNode(INode* node)
    {
        AddNode(node, false);
    }

    virtual void OnMyStringScalar(const TStringBuf& value)
    {
        auto node = Factory->CreateString();
        node->SetValue(Stroka(value));
        AddNode(~node, false);
    }

    virtual void OnMyIntegerScalar(i64 value)
    {

        auto node = Factory->CreateInteger();
        node->SetValue(value);
        AddNode(~node, false);
    }

    virtual void OnMyDoubleScalar(double value)
    {
        auto node = Factory->CreateDouble();
        node->SetValue(value);
        AddNode(~node, false);
    }

    virtual void OnMyEntity()
    {
        AddNode(~Factory->CreateEntity(), false);
    }


    virtual void OnMyBeginList()
    {
        AddNode(~Factory->CreateList(), true);
    }

    virtual void OnMyListItem()
    {
        YASSERT(!Key);
    }

    virtual void OnMyEndList()
    {
        NodeStack.pop();
    }


    virtual void OnMyBeginMap()
    {
        AddNode(~Factory->CreateMap(), true);
    }

    virtual void OnMyKeyedItem(const TStringBuf& key)
    {
        Key = Stroka(key);
    }

    virtual void OnMyEndMap()
    {
        NodeStack.pop();
    }

    virtual void OnMyBeginAttributes()
    {
        YASSERT(!AttributeConsumer);
        Attributes.Reset(CreateEphemeralAttributes().Release());
        AttributeConsumer.Reset(new TAttributeConsumer(Attributes.Get()));
        Forward(~AttributeConsumer, TClosure(), EYsonType::MapFragment);
    }

    virtual void OnMyEndAttributes()
    {
        AttributeConsumer.Reset(NULL);
        YASSERT(Attributes.Get());
    }

private:
    INodeFactory* Factory;
    //! Contains nodes forming the current path in the tree.
    std::stack<INodePtr> NodeStack;
    TNullable<Stroka> Key;
    INodePtr ResultNode;
    THolder<TAttributeConsumer> AttributeConsumer;
    THolder<IAttributeDictionary> Attributes;

    void AddNode(INode* node, bool push)
    {
        if (Attributes.Get()) {
            node->Attributes().MergeFrom(*Attributes);
            Attributes.Reset(NULL);
        }

        if (NodeStack.empty()) {
            ResultNode = node;
        } else {
            auto collectionNode = NodeStack.top();
            if (Key) {
                YVERIFY(collectionNode->AsMap()->AddChild(node, *Key));
                Key.Reset();
            } else {
                collectionNode->AsList()->AddChild(node);
            }
        }

        if (push) {
            NodeStack.push(node);
        }
    }
};

TAutoPtr<ITreeBuilder> CreateBuilderFromFactory(INodeFactory* factory)
{
    return new TTreeBuilder(factory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
