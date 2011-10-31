#pragma once

#include "common.h"
#include "ypath.h"
#include "yson_events.h"
#include "tree_builder.h"
#include "forwarding_yson_events.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public TForwardingYsonConsumer
{
protected:
    TNodeSetterBase(INode::TPtr node);

    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnMyDoubleScalar(double value, bool hasAttributes);
    virtual void OnMyEntity(bool hasAttributes);

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList(bool hasAttributes);


    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const Stroka& name);
    virtual void OnMyEndMap(bool hasAttributes);

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const Stroka& name);
    virtual void OnMyEndAttributes();

private:
    typedef TNodeSetterBase TThis;

    INode::TPtr Node;
    Stroka AttributeName;
    TAutoPtr<TTreeBuilder> AttributeBuilder;

    void OnForwardingFinished();

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define DECLARE_SCALAR_TYPE(name, type) \
    template <> \
    class TNodeSetter<I##name##Node> \
        : public TNodeSetterBase \
    { \
    public: \
        TNodeSetter(I##name##Node::TPtr node) \
            : TNodeSetterBase(~node) \
            , Node(node) \
        { } \
    \
    private: \
        I##name##Node::TPtr Node; \
        \
        virtual ENodeType GetExpectedType() \
        { \
            return ENodeType::name; \
        } \
        \
        virtual void On ## name ## Scalar( \
            NDetail::TScalarTypeTraits<type>::TParamType value, \
            bool hasAttributes) \
        { \
            UNUSED(hasAttributes); \
            Node->SetValue(value); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64,  i64);
DECLARE_SCALAR_TYPE(Double, double);

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IMapNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IMapNode::TPtr map)
        : TNodeSetterBase(~map)
        , Map(map)
    { }

private:
    typedef TNodeSetter<IMapNode> TThis;

    IMapNode::TPtr Map;
    Stroka ItemName;
    TAutoPtr<TTreeBuilder> ItemBuilder;

    virtual ENodeType GetExpectedType()
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap()
    {
        Map->Clear();
    }

    virtual void OnMyMapItem(const Stroka& name)
    {
        YASSERT(~ItemBuilder == NULL);
        ItemName = name;
        ItemBuilder.Reset(new TTreeBuilder(Map->GetFactory()));
        ForwardNode(~ItemBuilder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YASSERT(~ItemBuilder != NULL);
        Map->AddChild(ItemBuilder->GetRoot(), ItemName);
        ItemBuilder.Destroy();
        ItemName.clear();
    }

    virtual void OnMyEndMap(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IListNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IListNode::TPtr list)
        : TNodeSetterBase(~list)
        , List(list)
    { }

private:
    typedef TNodeSetter<IListNode> TThis;

    IListNode::TPtr List;
    TAutoPtr<TTreeBuilder> ItemBuilder;

    virtual ENodeType GetExpectedType()
    {
        return ENodeType::List;
    }

    virtual void OnMyBeginList()
    {
        List->Clear();
    }

    virtual void OnMyListItem()
    {
        YASSERT(~ItemBuilder == NULL);
        ItemBuilder.Reset(new TTreeBuilder(List->GetFactory()));
        ForwardNode(~ItemBuilder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YASSERT(~ItemBuilder != NULL);
        List->AddChild(ItemBuilder->GetRoot());
        ItemBuilder.Destroy();
    }

    virtual void OnMyEndList(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IEntityNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IEntityNode::TPtr entity)
        : TNodeSetterBase(~entity)
    { }

private:
    virtual ENodeType GetExpectedType()
    {
        return ENodeType::Entity;
    }

    virtual void OnMyEntity(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
void SetNodeFromProducer(TIntrusivePtr<TNode> node, TYsonProducer::TPtr producer)
{
    YASSERT(~node != NULL);
    YASSERT(~producer != NULL);

    TNodeSetter<TNode> setter(node);
    producer->Do(&setter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
