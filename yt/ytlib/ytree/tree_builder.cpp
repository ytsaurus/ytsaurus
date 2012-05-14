#include "stdafx.h"
#include "tree_builder.h"

#include "attributes.h"
#include "forwarding_yson_consumer.h"

#include <ytlib/actions/bind.h>
#include <ytlib/misc/assert.h>

#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TAttributeConsumer
    : public TForwardingYsonConsumer
{
public:
    TAttributeConsumer()
        : Attributes(CreateEphemeralAttributes())
        , Output(Value)
        , Writer(&Output)
    { }

    const IAttributeDictionary& GetAttributes() const
    {
        return *Attributes;
    }

    virtual void OnMyKeyedItem(const TStringBuf& key)
    {
        Key = key;
        ForwardNode(&Writer, BIND([=] () mutable {
            Attributes->SetYson(Key, Value);
            // TODO(babenko): "this" is needed by VC
            this->Key.clear();
            this->Value.clear();
        }));
    }

private:
    THolder<IAttributeDictionary> Attributes;
    TStringOutput Output;
    TYsonWriter Writer;

    Stroka Key;
    TYson Value;
};

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
        AttributeConsumer.Reset(new TAttributeConsumer());
        ForwardFragment(~AttributeConsumer);
    }

    virtual void OnMyEndAttributes()
    {
        YASSERT(AttributeConsumer.Get());
    }

private:
    INodeFactory* Factory;
    //! Contains nodes forming the current path in the tree.
    std::stack<INodePtr> NodeStack;
    TNullable<Stroka> Key;
    INodePtr ResultNode;
    THolder<TAttributeConsumer> AttributeConsumer;

    void AddNode(INode* node, bool push)
    {
        if (AttributeConsumer.Get()) {
            node->Attributes().MergeFrom(AttributeConsumer->GetAttributes());
            AttributeConsumer.Reset(NULL);
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
