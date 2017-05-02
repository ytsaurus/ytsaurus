#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

//! Implements IAsyncOutputStream interface on top of a file descriptor.
class TAsyncWriter
    : public NConcurrency::IAsyncOutputStream
{
public:
    //! Takes ownership of #fd.
    explicit TAsyncWriter(int fd);

    explicit TAsyncWriter(TNamedPipePtr ptr);

    virtual ~TAsyncWriter();

    int GetHandle() const;

    virtual TFuture<void> Write(const TSharedRef& buffer) override;

    virtual TFuture<void> Close() override;

    //! Thread-safe, can be called multiple times.
    TFuture<void> Abort();

    //! Time spent waiting for write requests.
    TFuture<TDuration> GetIdleDuration() const;

    //! Time spent waiting for the pipe readiness and doing actual writes.
    TFuture<TDuration> GetBusyDuration() const;

    //! Number of bytes written so far.
    i64 GetByteCount() const;

private:
    NDetail::TAsyncWriterImplPtr Impl_;
    TNamedPipePtr NamedPipeHolder_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
