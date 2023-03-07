#pragma once

#include <yt/core/yson/consumer.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TLimitedYsonWriter
    : public NYson::IYsonConsumer
{
public:
    TLimitedYsonWriter(i64 limit, NYson::EYsonFormat ysonFormat);

    virtual ~TLimitedYsonWriter();

    const TString& GetResult() const;

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
    virtual void OnKeyedItem(TStringBuf name) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;
    virtual void OnRaw(TStringBuf yson, NYson::EYsonType type) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
