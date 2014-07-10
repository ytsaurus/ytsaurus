#pragma once

#include "public.h"

#include <core/misc/blob.h>

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncReader
    : public NConcurrency::IAsyncInputStream
{
public:
    // Owns this fd
    explicit TAsyncReader(int fd);
    TAsyncReader(const TAsyncReader& other);
    ~TAsyncReader();

    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) override;

    TError Abort();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
