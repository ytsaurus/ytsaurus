#pragma once

#include "public.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

//! Implements IAsyncInputStream interface on top of a file descriptor.
class TAsyncReader
    : public NConcurrency::IAsyncInputStream
{
public:
    // Takes ownership of #fd.
    explicit TAsyncReader(int fd);

    explicit TAsyncReader(TNamedPipePtr ptr);

    virtual ~TAsyncReader();

    int GetHandle() const;

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override;

    //! Thread-safe, can be called multiple times.
    TFuture<void> Abort();

    //! Time spent waiting for read requests.
    TFuture<TDuration> GetIdleDuration() const;

    //! Time spent waiting for data in the pipe and doing actual reads.
    TFuture<TDuration> GetBusyDuration() const;

    //! Nubmer of bytes read so far.
    i64 GetByteCount() const;

private:
    NDetail::TAsyncReaderImplPtr Impl_;
    TNamedPipePtr NamedPipeHolder_;

};

DEFINE_REFCOUNTED_TYPE(TAsyncReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
