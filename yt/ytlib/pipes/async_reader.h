#pragma once

#include "public.h"

#include <core/concurrency/async_stream.h>

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

    virtual TFuture<TErrorOr<size_t>> Read(void* buffer, size_t length) override;

    //! Thread-safe, can be called multiple times.
    TFuture<void> Abort();

private:
    NDetail::TAsyncReaderImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TAsyncReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
