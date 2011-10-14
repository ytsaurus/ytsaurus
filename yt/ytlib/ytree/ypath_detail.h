#pragma once

#include "common.h"
#include "ypath.h"
#include "yson_events.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public IYsonConsumer
{
protected:
    IYsonConsumer* FwdConsumer;
    int FwdDepth;

    TNodeSetterBase();

    void InvalidType();

    void SetFwdConsumer(IYsonConsumer* consumer);
    virtual void OnFwdConsumerFinished();

    virtual void OnStringScalar(const Stroka& value);
    virtual void OnInt64Scalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntityScalar();


    virtual void OnBeginList();
    virtual void OnListItem(int index);
    virtual void OnEndList();


    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap();


    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

    void UpdateFwdDepth(int depthDelta);

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define DECLARE_SCALAR_TYPE(name, type) \
    template <> \
    class TNodeSetter<I##name##Node> : \
        public TNodeSetterBase \
    { \
    public: \
        TNodeSetter(I##name##Node::TPtr node) \
            : Node(node) \
        { } \
    \
    private: \
        I##name##Node::TPtr Node; \
        \
        virtual void On ## name ## Scalar(TScalarTypeTraits<type>::TParamType value) \
        { \
            Node->SetValue(value); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64, i64);
DECLARE_SCALAR_TYPE(Double, double);

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IMapNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IMapNode::TPtr map)
        : Map(map)
    { }

private:
    IMapNode::TPtr Map;
    
    Stroka ItemName;
    TAutoPtr<TTreeBuilder> ItemBuilder;

    virtual void OnBeginMap()
    {
        if (FwdConsumer == NULL) {
            Map->Clear();
        } else {
            TNodeSetterBase::OnBeginMap();
        }
    }

    virtual void OnMapItem(const Stroka& name)
    {
        if (FwdConsumer == NULL) {
            YASSERT(~ItemBuilder == NULL);
            ItemName = name;
            ItemBuilder.Reset(new TTreeBuilder(Map->GetFactory()));
            SetFwdConsumer(~ItemBuilder);
        } else {
            TNodeSetterBase::OnMapItem(name);
        }
    }

    virtual void OnFwdConsumerFinished()
    {
        YASSERT(~ItemBuilder != NULL);
        Map->AddChild(ItemBuilder->GetRoot(), ItemName);
        ItemBuilder.Destroy();
        ItemName.clear();
    }

    virtual void OnEndMap()
    {
        if (FwdConsumer == NULL) {
            // Just do nothing.
        } else {
            TNodeSetterBase::OnEndMap();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IListNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IListNode::TPtr list)
        : List(list)
    { }

private:
    IListNode::TPtr List;

    TAutoPtr<TTreeBuilder> ItemBuilder;

    virtual void OnBeginList()
    {
        if (FwdConsumer == NULL) {
            List->Clear();
        } else {
            TNodeSetterBase::OnEndMap();
        }
    }

    virtual void OnListItem(int index)
    {
        if (FwdConsumer == NULL) {
            YASSERT(~ItemBuilder == NULL);
            ItemBuilder.Reset(new TTreeBuilder(List->GetFactory()));
            SetFwdConsumer(~ItemBuilder);
        } else {
            TNodeSetterBase::OnListItem(index);
        }
    }

    virtual void OnFwdConsumerFinished()
    {
        YASSERT(~ItemBuilder != NULL);
        List->AddChild(ItemBuilder->GetRoot());
        ItemBuilder.Destroy();
    }

    virtual void OnEndList()
    {
        if (FwdConsumer == NULL) {
            // Just do nothing.
        } else {
            TNodeSetterBase::OnEndMap();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IEntityNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IEntityNode::TPtr)
    { }

private:
    void OnEntityScalar()
    { }
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
