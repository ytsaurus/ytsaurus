#pragma once

#include "string.h"

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual TYsonConsumerBase
{
public:
    // IYsonConsumer methods
    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    virtual void OnRaw(TStringBuf yson, EYsonType type) override;

protected:
    void Forward(
        IYsonConsumer* consumer,
        std::function<void()> onFinished = nullptr,
        EYsonType type = EYsonType::Node);

    virtual void OnMyStringScalar(TStringBuf value);
    virtual void OnMyInt64Scalar(i64 value);
    virtual void OnMyUint64Scalar(ui64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyBooleanScalar(bool value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyKeyedItem(TStringBuf key);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

    virtual void OnMyRaw(TStringBuf yson, EYsonType type);

private:
    IYsonConsumer* ForwardingConsumer_ = nullptr;
    int ForwardingDepth_ = 0;
    EYsonType ForwardingType_;
    std::function<void()> OnFinished_;

    bool CheckForwarding(int depthDelta = 0);
    void UpdateDepth(int depthDelta, bool checkFinish = true);
    void FinishForwarding();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
