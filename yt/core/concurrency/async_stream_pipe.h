#pragma once

#include "public.h"

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/nonblocking_queue.h>


namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

class TAsyncStreamPipe
    : public IAsyncZeroCopyInputStream
    , public IAsyncClosableOutputStream
{
public:
    virtual TFuture<TSharedRef> Read() override;

    virtual TFuture<void> Write(const TSharedRef& buffer) override;
    virtual TFuture<void> Close() override;

private:
    struct TItem
    {
        // If SharedRef_ is empty it means close was requested.
        TSharedRef SharedRef_;
        TPromise<void> WriteComplete_;

        TItem(TSharedRef sharedRef, TPromise<void> writeComplete);
    };

    TNonblockingQueue<TItem> Queue_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncStreamPipe);

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
