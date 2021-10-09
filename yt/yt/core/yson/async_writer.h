#pragma once

#include "public.h"
#include "async_consumer.h"
#include "writer.h"
#include "string.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TAsyncYsonWriter
    : public IAsyncYsonConsumer
    , private TNonCopyable
{
public:
    explicit TAsyncYsonWriter(EYsonType type = EYsonType::Node);

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
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, EYsonType type) override;
    void OnRaw(TFuture<TYsonString> asyncStr) override;

    TFuture<TYsonString> Finish(IInvokerPtr invoker = nullptr);

private:
    const EYsonType Type_;

    TStringStream Stream_;
    TBufferedBinaryYsonWriter SyncWriter_;

    using TSegment = std::pair<TYsonString, bool>;
    std::vector<TFuture<TSegment>> AsyncSegments_;


    void FlushCurrentSegment();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

