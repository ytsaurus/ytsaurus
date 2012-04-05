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

    virtual void OnMyStringScalar(const TStringBuf& value, bool hasAttributes);
    virtual void OnMyIntegerScalar(i64 value, bool hasAttributes);
    virtual void OnMyDoubleScalar(double value, bool hasAttributes);
    virtual void OnMyEntity(bool hasAttributes);

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList(bool hasAttributes);

    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const TStringBuf& name);
    virtual void OnMyEndMap(bool hasAttributes);

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const TStringBuf& name);
    virtual void OnMyEndAttributes();

private:
    IYsonConsumer* ForwardingConsumer;
    int ForwardingDepth;
    TClosure OnForwardingFinished;

    virtual void OnStringScalar(const TStringBuf& value, bool hasAttributes);
    virtual void OnIntegerScalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList(bool hasAttributes);

    virtual void OnBeginMap();
    virtual void OnMapItem(const TStringBuf& name);
    virtual void OnEndMap(bool hasAttributes);

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const TStringBuf& name);
    virtual void OnEndAttributes();

    void DoForward(IYsonConsumer* consumer, const TClosure& onForwardingFinished, int depth);
    void UpdateDepth(int depthDelta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
