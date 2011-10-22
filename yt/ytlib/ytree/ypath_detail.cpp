#include "stdafx.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TForwardingYsonConsumer::TForwardingYsonConsumer()
    : ForwardingConsumer(NULL)
    , ForwardingDepth(0)
{ }

void TForwardingYsonConsumer::StartForwarding(
    IYsonConsumer* consumer,
    IAction::TPtr onForwardingFinished)
{
    YASSERT(ForwardingConsumer == NULL);
    YASSERT(consumer != NULL);

    ForwardingConsumer = consumer;
    ForwardingDepth = 0;
    OnForwardingFinished = onForwardingFinished;
}

void TForwardingYsonConsumer::UpdateFwdDepth(int depthDelta)
{
    ForwardingDepth += depthDelta;
    YASSERT(ForwardingDepth >= 0);
    if (ForwardingDepth == 0) {
        ForwardingConsumer = NULL;
        if (~OnForwardingFinished != NULL) {
            OnForwardingFinished->Do();
            OnForwardingFinished.Drop();
        }
    }
}

void TForwardingYsonConsumer::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyStringScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnStringScalar(value, hasAttributes);
    }
}

void TForwardingYsonConsumer::OnInt64Scalar(i64 value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyInt64Scalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnInt64Scalar(value, hasAttributes);
    }
}

void TForwardingYsonConsumer::OnDoubleScalar(double value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyDoubleScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnDoubleScalar(value, hasAttributes);
    }
}

void TForwardingYsonConsumer::OnEntity(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEntity(hasAttributes);
    } else {
        ForwardingConsumer->OnEntity(hasAttributes);
    }
}

void TForwardingYsonConsumer::OnBeginList()
{
    if (ForwardingConsumer == NULL) {
        OnMyBeginList();
    } else {
        ForwardingConsumer->OnBeginList();
        UpdateFwdDepth(+1);
    }
}

void TForwardingYsonConsumer::OnListItem()
{
    if (ForwardingConsumer == NULL) {
        OnMyListItem();
    } else {
        ForwardingConsumer->OnListItem();
        UpdateFwdDepth(0);
    }
}

void TForwardingYsonConsumer::OnEndList(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEndList(hasAttributes);
    } else {
        ForwardingConsumer->OnEndList(hasAttributes);
        if (!hasAttributes) {
            UpdateFwdDepth(-1);
        }
    }
}

void TForwardingYsonConsumer::OnBeginMap()
{
    if (ForwardingConsumer == NULL) {
        OnMyBeginMap();
    } else {
        ForwardingConsumer->OnBeginMap();
        UpdateFwdDepth(+1);
    }
}

void TForwardingYsonConsumer::OnMapItem(const Stroka& name)
{
    if (ForwardingConsumer == NULL) {
        OnMapItem(name);
    } else {
        ForwardingConsumer->OnMapItem(name);
        UpdateFwdDepth(0);
    }
}

void TForwardingYsonConsumer::OnEndMap(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEndMap(hasAttributes);
    } else {
        ForwardingConsumer->OnEndMap(hasAttributes);
        if (!hasAttributes) {
            UpdateFwdDepth(-1);
        }
    }
}

void TForwardingYsonConsumer::OnBeginAttributes()
{
    if (ForwardingConsumer == NULL) {
        OnMyBeginAttributes();
    } else {
        ForwardingConsumer->OnBeginAttributes();
    }
}

void TForwardingYsonConsumer::OnAttributesItem(const Stroka& name)
{
    if (ForwardingConsumer == NULL) {
        OnMyAttributesItem(name);
    } else {
        ForwardingConsumer->OnAttributesItem(name);
        UpdateFwdDepth(0);
    }
}

void TForwardingYsonConsumer::OnEndAttributes()
{
    if (ForwardingConsumer == NULL) {
        OnMyEndAttributes();
    } else {
        ForwardingConsumer->OnEndAttributes();
        UpdateFwdDepth(-1);
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode::TPtr node)
    : Node(node)
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    throw TYTreeException() << Sprintf("Cannot change node type from %s to %s",
        ~GetExpectedType().ToString().Quote(),
        ~actualType.ToString().Quote());
}

void TNodeSetterBase::OnMyStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Int64);
}

void TNodeSetterBase::OnMyDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Double);
}

void TNodeSetterBase::OnMyEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Entity);
}

void TNodeSetterBase::OnMyBeginList()
{
    ThrowInvalidType(ENodeType::List);
}

void TNodeSetterBase::OnMyListItem()
{
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

void TNodeSetterBase::OnMyMapItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndMap(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginAttributes()
{
    auto attributes = Node->GetAttributes();
    if (~attributes == NULL) {
        Node->SetAttributes(Node->GetFactory()->CreateMap());
    } else {
        attributes->Clear();
    }
}

void TNodeSetterBase::OnMyAttributesItem(const Stroka& name)
{
    YASSERT(~AttributeBuilder == NULL);
    AttributeName = name;
    AttributeBuilder.Reset(new TTreeBuilder(Node->GetFactory()));
    StartForwarding(~AttributeBuilder, FromMethod(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    YASSERT(~AttributeBuilder != NULL);
    Node->GetAttributes()->AddChild(AttributeBuilder->GetRoot(), AttributeName);
    AttributeBuilder.Destroy();
    AttributeName.clear();
}

void TNodeSetterBase::OnMyEndAttributes()
{
    if (Node->GetAttributes()->GetChildCount() == 0) {
        Node->SetAttributes(NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
