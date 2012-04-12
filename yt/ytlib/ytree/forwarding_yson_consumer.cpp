#include "stdafx.h"
#include "forwarding_yson_consumer.h"

#include <ytlib/misc/assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TForwardingYsonConsumer::TForwardingYsonConsumer()
    : ForwardingConsumer(NULL)
    , ForwardingDepth(0)
{ }

void TForwardingYsonConsumer::ForwardNode(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 0);
}

void TForwardingYsonConsumer::ForwardAttributes(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished)
{
    DoForward(consumer, onForwardingFinished, 1);
}

void TForwardingYsonConsumer::DoForward(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished,
    int depth)
{
    YASSERT(!ForwardingConsumer);
    YASSERT(consumer);

    ForwardingConsumer = consumer;
    ForwardingDepth = depth;
    OnForwardingFinished = onForwardingFinished;
}

void TForwardingYsonConsumer::UpdateDepth(int depthDelta, bool checkFinish)
{
    ForwardingDepth += depthDelta;
    YASSERT(ForwardingDepth >= 0);
    if (checkFinish && ForwardingDepth == 0) {
        ForwardingConsumer = NULL;
        if (!OnForwardingFinished.IsNull()) {
            OnForwardingFinished.Run();
            OnForwardingFinished.Reset();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TForwardingYsonConsumer::OnStringScalar(const TStringBuf& value)
{
    if (!ForwardingConsumer) {
        OnMyStringScalar(value);
    } else {
        ForwardingConsumer->OnStringScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnIntegerScalar(i64 value)
{
    if (!ForwardingConsumer) {
        OnMyIntegerScalar(value);
    } else {
        ForwardingConsumer->OnIntegerScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnDoubleScalar(double value)
{
    if (!ForwardingConsumer) {
        OnMyDoubleScalar(value);
    } else {
        ForwardingConsumer->OnDoubleScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnEntity()
{
    if (!ForwardingConsumer) {
        OnMyEntity();
    } else {
        ForwardingConsumer->OnEntity();
        UpdateDepth(0);
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

void TForwardingYsonConsumer::OnEndList()
{
    if (!ForwardingConsumer) {
        OnMyEndList();
    } else {
        ForwardingConsumer->OnEndList();
        UpdateDepth(-1);
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

void TForwardingYsonConsumer::OnMapItem(const TStringBuf& name)
{
    if (!ForwardingConsumer) {
        OnMyMapItem(name);
    } else {
        ForwardingConsumer->OnMapItem(name);
    }
}

void TForwardingYsonConsumer::OnEndMap()
{
    if (!ForwardingConsumer) {
        OnMyEndMap();
    } else {
        ForwardingConsumer->OnEndMap();
        UpdateDepth(-1);
    }
}

void TForwardingYsonConsumer::OnBeginAttributes()
{
    if (!ForwardingConsumer) {
        OnMyBeginAttributes();
    } else {
        ForwardingConsumer->OnBeginAttributes();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnAttributesItem(const TStringBuf& name)
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
        UpdateDepth(-1, false);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TForwardingYsonConsumer::OnMyStringScalar(const TStringBuf& value)
{
    UNUSED(value);

    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyIntegerScalar(i64 value)
{
    UNUSED(value);

    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyDoubleScalar(double value)
{
    UNUSED(value);

    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEntity()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyBeginList()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyListItem()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEndList()
{

    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyBeginMap()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyMapItem(const TStringBuf& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyEndMap()
{

    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyBeginAttributes()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyAttributesItem(const TStringBuf& name)
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
