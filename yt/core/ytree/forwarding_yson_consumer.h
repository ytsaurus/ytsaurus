#pragma once

#include "yson_string.h"
#include <core/yson/consumer.h>
#include <core/actions/callback.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual NYson::TYsonConsumerBase
{
public:
    // IYsonConsumer methods
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnInt64Scalar(i64 value);
    virtual void OnUint64Scalar(ui64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnBooleanScalar(bool value);
    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type);

protected:
    TForwardingYsonConsumer();

    void Forward(
        NYson::IYsonConsumer* consumer,
        const TClosure& onFinished = TClosure(),
        NYson::EYsonType type = NYson::EYsonType::Node);

    virtual void OnMyStringScalar(const TStringBuf& value);
    virtual void OnMyInt64Scalar(i64 value);
    virtual void OnMyUint64Scalar(ui64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyBooleanScalar(bool value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyKeyedItem(const TStringBuf& key);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

    virtual void OnMyRaw(const TStringBuf& yson, NYson::EYsonType type);

private:
    NYson::IYsonConsumer* ForwardingConsumer;
    int ForwardingDepth;
    NYson::EYsonType ForwardingType;
    TClosure OnFinished;

    bool CheckForwarding(int depthDelta = 0);
    void UpdateDepth(int depthDelta, bool checkFinish = true);
    void FinishForwarding();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
