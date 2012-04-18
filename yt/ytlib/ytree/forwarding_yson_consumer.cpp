#include "stdafx.h"
#include "forwarding_yson_consumer.h"

#include <ytlib/misc/assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TForwardingYsonConsumer::TForwardingYsonConsumer()
    : ForwardingConsumer(NULL)
    , ForwardingDepth(0)
    , ForwardingFragment(false)
{ }

void TForwardingYsonConsumer::ForwardNode(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished)
{
    StartForwarding(consumer, onForwardingFinished, false);
}

void TForwardingYsonConsumer::ForwardFragment(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished)
{
    StartForwarding(consumer, onForwardingFinished, true);
}

void TForwardingYsonConsumer::StartForwarding(
    IYsonConsumer* consumer,
    const TClosure& onForwardingFinished,
    bool forwardingFragment)
{
    YASSERT(!ForwardingConsumer);
    YASSERT(consumer);
    YASSERT(ForwardingDepth == 0);

    ForwardingConsumer = consumer;
    OnForwardingFinished = onForwardingFinished;
    ForwardingFragment = forwardingFragment;
}

bool TForwardingYsonConsumer::CheckForwarding(int depthDelta)
{
    if (ForwardingDepth + depthDelta < 0) {
        FinishForwarding();
    }
    return ForwardingConsumer;
}

void TForwardingYsonConsumer::UpdateDepth(int depthDelta, bool checkFinish)
{
    ForwardingDepth += depthDelta;
    YASSERT(ForwardingDepth >= 0);
    if (checkFinish && !ForwardingFragment && ForwardingDepth == 0) {
        FinishForwarding();
    }
}

void TForwardingYsonConsumer::FinishForwarding()
{
    ForwardingConsumer = NULL;
    if (!OnForwardingFinished.IsNull()) {
        OnForwardingFinished.Run();
        OnForwardingFinished.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TForwardingYsonConsumer::OnStringScalar(const TStringBuf& value)
{
    if (!CheckForwarding()) {
        OnMyStringScalar(value);
    } else {
        ForwardingConsumer->OnStringScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnIntegerScalar(i64 value)
{
    if (!CheckForwarding()) {
        OnMyIntegerScalar(value);
    } else {
        ForwardingConsumer->OnIntegerScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnDoubleScalar(double value)
{
    if (!CheckForwarding()) {
        OnMyDoubleScalar(value);
    } else {
        ForwardingConsumer->OnDoubleScalar(value);
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnEntity()
{
    if (!CheckForwarding()) {
        OnMyEntity();
    } else {
        ForwardingConsumer->OnEntity();
        UpdateDepth(0);
    }
}

void TForwardingYsonConsumer::OnBeginList()
{
    if (!CheckForwarding(+1)) {
        OnMyBeginList();
    } else {
        ForwardingConsumer->OnBeginList();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnListItem()
{
    if (!CheckForwarding()) {
        OnMyListItem();
    } else {
        ForwardingConsumer->OnListItem();
    }
}

void TForwardingYsonConsumer::OnEndList()
{
    if (!CheckForwarding(-1)) {
        OnMyEndList();
    } else {
        ForwardingConsumer->OnEndList();
        UpdateDepth(-1);
    }
}

void TForwardingYsonConsumer::OnBeginMap()
{
    if (!CheckForwarding(+1)) {
        OnMyBeginMap();
    } else {
        ForwardingConsumer->OnBeginMap();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnKeyedItem(const TStringBuf& name)
{
    if (!CheckForwarding()) {
        OnMyKeyedItem(name);
    } else {
        ForwardingConsumer->OnKeyedItem(name);
    }
}

void TForwardingYsonConsumer::OnEndMap()
{
    if (!CheckForwarding(-1)) {
        OnMyEndMap();
    } else {
        ForwardingConsumer->OnEndMap();
        UpdateDepth(-1);
    }
}

void TForwardingYsonConsumer::OnRaw(const TStringBuf &yson, EYsonType type)
{
    if (!CheckForwarding()) {
        OnMyRaw(yson, type);
    } else {
        YUNIMPLEMENTED();
    }
}

void TForwardingYsonConsumer::OnBeginAttributes()
{
    if (!CheckForwarding()) {
        OnMyBeginAttributes();
    } else {
        ForwardingConsumer->OnBeginAttributes();
        UpdateDepth(+1);
    }
}

void TForwardingYsonConsumer::OnEndAttributes()
{
    if (!CheckForwarding()) {
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

void TForwardingYsonConsumer::OnMyKeyedItem(const TStringBuf& name)
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

void TForwardingYsonConsumer::OnMyEndAttributes()
{
    YUNREACHABLE();
}

void TForwardingYsonConsumer::OnMyRaw(const TStringBuf& yson, EYsonType type)
{
    TYsonConsumerBase::OnRaw(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
