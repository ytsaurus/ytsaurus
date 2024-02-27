#pragma once

#include <yt/yt/core/yson/consumer.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TLimitedYsonWriter
    : public NYson::IYsonConsumer
{
public:
    TLimitedYsonWriter(i64 limit, NYson::EYsonFormat ysonFormat);

    virtual ~TLimitedYsonWriter();

    const TString& GetResult() const;

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf name) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, NYson::EYsonType type) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
