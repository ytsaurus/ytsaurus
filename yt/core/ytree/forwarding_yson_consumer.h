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
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity();

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

protected:
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
    NYson::IYsonConsumer* ForwardingConsumer_ = nullptr;
    int ForwardingDepth_ = 0;
    NYson::EYsonType ForwardingType_;
    TClosure OnFinished_;

    bool CheckForwarding(int depthDelta = 0);
    void UpdateDepth(int depthDelta, bool checkFinish = true);
    void FinishForwarding();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
