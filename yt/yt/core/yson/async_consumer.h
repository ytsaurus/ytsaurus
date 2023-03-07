#pragma once

#include "public.h"
#include "string.h"
#include "consumer.h"

#include <yt/core/actions/future.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Extends IYsonConsumer by enabling asynchronously constructed
//! segments to be injected into the stream.
struct IAsyncYsonConsumer
    : public IYsonConsumer
{
    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Turns IYsonConsumer into IAsyncConsumer. No asynchronous calls are allowed.
class TAsyncYsonConsumerAdapter
    : public IAsyncYsonConsumer
{
public:
    explicit TAsyncYsonConsumerAdapter(IYsonConsumer* underlyingConsumer);

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
    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TStringBuf yson, EYsonType type) override;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) override;

private:
    IYsonConsumer* const UnderlyingConsumer_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

