#pragma once

#include "public.h"
#include "async_consumer.h"
#include "writer.h"
#include "string.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TAsyncYsonWriter
    : public IAsyncYsonConsumer
    , private TNonCopyable
{
public:
    explicit TAsyncYsonWriter(EYsonType type = EYsonType::Node);

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
    virtual void OnRaw(TFuture<TYsonString> asyncStr) override;

    TFuture<TYsonString> Finish();

private:
    const EYsonType Type_;

    TStringStream Stream_;
    TBufferedBinaryYsonWriter SyncWriter_;

    std::vector<TFuture<TString>> AsyncSegments_;


    void FlushCurrentSegment();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

