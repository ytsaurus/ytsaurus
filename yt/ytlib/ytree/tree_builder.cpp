#include "stdafx.h"
#include "tree_builder.h"
#include "attributes.h"
#include "forwarding_yson_consumer.h"
#include "attribute_consumer.h"
#include "node.h"
#include "attribute_helpers.h"

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
    explicit TTreeBuilder(INodeFactoryPtr factory)
        : Factory(factory)
    { }

    virtual void BeginTree() override
    {
        YCHECK(NodeStack.size() == 0);
    }

    virtual INodePtr EndTree() override
    {
        // Failure here means that the tree is not fully constructed yet.
        YCHECK(NodeStack.size() == 0);
        YCHECK(ResultNode);

        return ResultNode;
    }

    virtual void OnNode(INodePtr node) override
    {
        AddNode(node, false);
    }

    virtual void OnMyStringScalar(const TStringBuf& value) override
    {
        auto node = Factory->CreateString();
        node->SetValue(Stroka(value));
        AddNode(node, false);
    }

    virtual void OnMyIntegerScalar(i64 value) override
    {

        auto node = Factory->CreateInteger();
        node->SetValue(value);
        AddNode(node, false);
    }

    virtual void OnMyDoubleScalar(double value) override
    {
        auto node = Factory->CreateDouble();
        node->SetValue(value);
        AddNode(node, false);
    }

    virtual void OnMyEntity() override
    {
        AddNode(Factory->CreateEntity(), false);
    }


    virtual void OnMyBeginList() override
    {
        AddNode(Factory->CreateList(), true);
    }

    virtual void OnMyListItem() override
    {
        YASSERT(!Key);
    }

    virtual void OnMyEndList() override
    {
        NodeStack.pop();
    }


    virtual void OnMyBeginMap() override
    {
        AddNode(Factory->CreateMap(), true);
    }

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        Key = Stroka(key);
    }

    virtual void OnMyEndMap() override
    {
        NodeStack.pop();
    }

    virtual void OnMyBeginAttributes() override
    {
        YASSERT(!AttributeConsumer);
        Attributes.Reset(CreateEphemeralAttributes().Release());
        AttributeConsumer.Reset(new TAttributeConsumer(Attributes.Get()));
        Forward(~AttributeConsumer, TClosure(), EYsonType::MapFragment);
    }

    virtual void OnMyEndAttributes() override
    {
        AttributeConsumer.Reset(NULL);
        YASSERT(Attributes.Get());
    }

private:
    INodeFactoryPtr Factory;
    //! Contains nodes forming the current path in the tree.
    std::stack<INodePtr> NodeStack;
    TNullable<Stroka> Key;
    INodePtr ResultNode;
    THolder<TAttributeConsumer> AttributeConsumer;
    THolder<IAttributeDictionary> Attributes;

    void AddNode(INodePtr node, bool push)
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
                if (!collectionNode->AsMap()->AddChild(node, *Key)) {
                    THROW_ERROR_EXCEPTION("Duplicate key %s", ~(*Key).Quote());
                }
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

TAutoPtr<ITreeBuilder> CreateBuilderFromFactory(INodeFactoryPtr factory)
{
    return new TTreeBuilder(factory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
