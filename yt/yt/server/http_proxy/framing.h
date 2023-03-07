#pragma once

#include "private.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Stream that wraps every written chunk in a frame.
/*!
 *  It is guaranteed to be thread-safe and reentrant.
 */
class TFramingAsyncOutputStream
    : public NConcurrency::IFlushableAsyncOutputStream
{
public:
    explicit TFramingAsyncOutputStream(NConcurrency::IFlushableAsyncOutputStreamPtr underlying);
    TFuture<void> WriteDataFrame(const TSharedRef& buffer);
    TFuture<void> WriteKeepAliveFrame();

    virtual TFuture<void> Write(const TSharedRef& buffer) override;
    virtual TFuture<void> Flush() override;
    virtual TFuture<void> Close() override;

private:
    NConcurrency::IFlushableAsyncOutputStreamPtr Underlying_;
    TFuture<void> PendingOperationFuture_ = VoidFuture;
    bool Closed_ = false;
    TSpinLock SpinLock_;

private:
    TFuture<void> DoWriteFrame(TString header, const std::optional<TSharedRef>& frame);
};

DEFINE_REFCOUNTED_TYPE(TFramingAsyncOutputStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy