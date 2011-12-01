#include "stdafx.h"
#include "forwarding_yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TForwardingYsonConsumer::TForwardingYsonConsumer()
    : ForwardingConsumer(NULL)
    , ForwardingDepth(0)
{ }

void TForwardingYsonConsumer::ForwardNode(
    IYsonConsumer* consumer,
    IAction::TPtr onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 0);
}

void TForwardingYsonConsumer::ForwardAttributes(
    IYsonConsumer* consumer,
    IAction::TPtr onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 1);
}

void TForwardingYsonConsumer::DoForward(
    IYsonConsumer* consumer,
    IAction::TPtr onForwardingFinished,
    int depth)
{
    YASSERT(ForwardingConsumer == NULL);
    YASSERT(consumer != NULL);

    ForwardingConsumer = consumer;
    ForwardingDepth = depth;
    OnForwardingFinished = onForwardingFinished;
}

void TForwardingYsonConsumer::UpdateDepth(int depthDelta)
{
    ForwardingDepth += depthDelta;
    YASSERT(ForwardingDepth >= 0);
    if (ForwardingDepth == 0) {
        ForwardingConsumer = NULL;
        if (~OnForwardingFinished != NULL) {
            OnForwardingFinished->Do();
            OnForwardingFinished.Reset();
        }
    }
}

void TForwardingYsonConsumer::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyStringScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnStringScalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnInt64Scalar(i64 value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyInt64Scalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnInt64Scalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnDoubleScalar(double value, bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyDoubleScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnDoubleScalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnEntity(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEntity(hasAttributes);
    } else {
        ForwardingConsumer->OnEntity(hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnBeginList()
{
    if (ForwardingConsumer == NULL) {
        OnMyBeginList();
    } else {
        ForwardingConsumer->OnBeginList();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnListItem()
{
    if (ForwardingConsumer == NULL) {
        OnMyListItem();
    } else {
        ForwardingConsumer->OnListItem();
    }
}

void TForwardingYsonConsumer::OnEndList(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEndList(hasAttributes);
    } else {
        ForwardingConsumer->OnEndList(hasAttributes);
        UpdateDepth(hasAttributes ? 0 : -1);
    }
}

void TForwardingYsonConsumer::OnBeginMap()
{
    if (ForwardingConsumer == NULL) {
        OnMyBeginMap();
    } else {
        ForwardingConsumer->OnBeginMap();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnMapItem(const Stroka& name)
{
    if (ForwardingConsumer == NULL) {
        OnMyMapItem(name);
    } else {
        ForwardingConsumer->OnMapItem(name);
    }
}

void TForwardingYsonConsumer::OnEndMap(bool hasAttributes)
{
    if (ForwardingConsumer == NULL) {
        OnMyEndMap(hasAttributes);
    } else {
        ForwardingConsumer->OnEndMap(hasAttributes);
        UpdateDepth(hasAttributes ? 0 : -1);
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
    }
}

void TForwardingYsonConsumer::OnEndAttributes()
{
    if (ForwardingConsumer == NULL) {
        OnMyEndAttributes();
    } else {
        ForwardingConsumer->OnEndAttributes();
        UpdateDepth(-1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
