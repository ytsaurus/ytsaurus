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
    TAsyncYsonWriter(
        EYsonFormat format = EYsonFormat::Binary,
        EYsonType type = EYsonType::Node,
        bool enableRaw = false,
        bool booleanAsString = false,
        int indent = 4);

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;
    virtual void OnRaw(const TStringBuf& yson, EYsonType type) override;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) override;

    TFuture<TYsonString> Finish();

private:
    TStringStream Stream_;
    TYsonWriter SyncWriter_;

    std::vector<TFuture<TYsonString>> AsyncSegments_;


    void FlushCurrentSegment();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

