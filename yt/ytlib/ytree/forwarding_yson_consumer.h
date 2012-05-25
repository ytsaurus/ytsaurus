#pragma once

#include "yson_consumer.h"

#include <ytlib/actions/callback.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual TYsonConsumerBase
{
protected:
    TForwardingYsonConsumer();

    void Forward(
        IYsonConsumer* consumer,
        const TClosure& onFinished = TClosure(),
        EYsonType type = EYsonType::Node);

    virtual void OnMyStringScalar(const TStringBuf& value);
    virtual void OnMyIntegerScalar(i64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyKeyedItem(const TStringBuf& key);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

    virtual void OnMyRaw(const TStringBuf& yson, EYsonType type);

    // IYsonConsumer methods
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

    virtual void OnRaw(const TStringBuf& yson, EYsonType type);

private:
    IYsonConsumer* ForwardingConsumer;
    int ForwardingDepth;
    EYsonType ForwardingType;
    TClosure OnFinished;

    bool CheckForwarding(int depthDelta = 0);
    void UpdateDepth(int depthDelta, bool checkFinish = true);
    void FinishForwarding();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
