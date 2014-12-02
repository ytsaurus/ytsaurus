#pragma once

#include "public.h"

#include <core/misc/blob.h>

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    class TAsyncReaderImpl;
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncReader
    : public NConcurrency::IAsyncInputStream
{
public:
    // Owns this fd
    explicit TAsyncReader(int fd);

    virtual TFuture<TErrorOr<size_t>> Read(void* buffer, size_t length) override;

    //! Thread-safe, can be called multiple times.
    TFuture<void> Abort();

private:
    TIntrusivePtr<NDetail::TAsyncReaderImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
