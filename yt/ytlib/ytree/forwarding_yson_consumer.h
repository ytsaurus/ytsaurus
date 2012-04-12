#pragma once

#include "yson_consumer.h"

#include <ytlib/actions/callback.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual IYsonConsumer
{
protected:
    TForwardingYsonConsumer();

    void ForwardNode(IYsonConsumer* consumer, const TClosure& onForwardingFinished);
    void ForwardAttributes(IYsonConsumer* consumer, const TClosure& onForwardingFinished);

    virtual void OnMyStringScalar(const TStringBuf& value);
    virtual void OnMyIntegerScalar(i64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const TStringBuf& name);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const TStringBuf& name);
    virtual void OnMyEndAttributes();

private:
    IYsonConsumer* ForwardingConsumer;
    int ForwardingDepth;
    TClosure OnForwardingFinished;

    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnMapItem(const TStringBuf& name);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const TStringBuf& name);
    virtual void OnEndAttributes();

    void DoForward(IYsonConsumer* consumer, const TClosure& onForwardingFinished, int depth);
    void UpdateDepth(int depthDelta, bool checkFinish = true);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
