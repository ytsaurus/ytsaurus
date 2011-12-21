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
    IAction* onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 0);
}

void TForwardingYsonConsumer::ForwardAttributes(
    IYsonConsumer* consumer,
    IAction* onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 1);
}

void TForwardingYsonConsumer::DoForward(
    IYsonConsumer* consumer,
    IAction* onForwardingFinished,
    int depth)
{
    YASSERT(!ForwardingConsumer);
    YASSERT(consumer);

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
        if (OnForwardingFinished) {
            OnForwardingFinished->Do();
            OnForwardingFinished.Reset();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TForwardingYsonConsumer::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyStringScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnStringScalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnInt64Scalar(i64 value, bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyInt64Scalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnInt64Scalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnDoubleScalar(double value, bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyDoubleScalar(value, hasAttributes);
    } else {
        ForwardingConsumer->OnDoubleScalar(value, hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnEntity(bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyEntity(hasAttributes);
    } else {
        ForwardingConsumer->OnEntity(hasAttributes);
        UpdateDepth(hasAttributes ? +1 : 0);
    }
}

void TForwardingYsonConsumer::OnBeginList()
{
    if (!ForwardingConsumer) {
        OnMyBeginList();
    } else {
        ForwardingConsumer->OnBeginList();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnListItem()
{
    if (!ForwardingConsumer) {
        OnMyListItem();
    } else {
        ForwardingConsumer->OnListItem();
    }
}

void TForwardingYsonConsumer::OnEndList(bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyEndList(hasAttributes);
    } else {
        ForwardingConsumer->OnEndList(hasAttributes);
        UpdateDepth(hasAttributes ? 0 : -1);
    }
}

void TForwardingYsonConsumer::OnBeginMap()
{
    if (!ForwardingConsumer) {
        OnMyBeginMap();
    } else {
        ForwardingConsumer->OnBeginMap();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnMapItem(const Stroka& name)
{
    if (!ForwardingConsumer) {
        OnMyMapItem(name);
    } else {
        ForwardingConsumer->OnMapItem(name);
    }
}

void TForwardingYsonConsumer::OnEndMap(bool hasAttributes)
{
    if (!ForwardingConsumer) {
        OnMyEndMap(hasAttributes);
    } else {
        ForwardingConsumer->OnEndMap(hasAttributes);
        UpdateDepth(hasAttributes ? 0 : -1);
    }
}

void TForwardingYsonConsumer::OnBeginAttributes()
{
    if (!ForwardingConsumer) {
        OnMyBeginAttributes();
    } else {
        ForwardingConsumer->OnBeginAttributes();
    }
}

void TForwardingYsonConsumer::OnAttributesItem(const Stroka& name)
{
    if (!ForwardingConsumer) {
        OnMyAttributesItem(name);
    } else {
        ForwardingConsumer->OnAttributesItem(name);
    }
}

void TForwardingYsonConsumer::OnEndAttributes()
{
    if (!ForwardingConsumer) {
        OnMyEndAttributes();
    } else {
        ForwardingConsumer->OnEndAttributes();
        UpdateDepth(-1);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TForwardingYsonConsumer::OnMyStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
}

void TForwardingYsonConsumer::OnMyBeginList()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyListItem()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyBeginMap()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyMapItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEndMap(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyBeginAttributes()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyAttributesItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEndAttributes()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
