#pragma once

#include "public.h"

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    DECLARE_REFCOUNTED_CLASS(TAsyncWriterImpl)
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriter
    : public NConcurrency::IAsyncOutputStream
{
public:
    // Owns this fd
    explicit TAsyncWriter(int fd);

    virtual TAsyncError Write(const void* data, size_t size) override;
    
    TAsyncError Close();

    //! Thread-safe, can be called multiple times.
    TFuture<void> Abort();

private:
    NDetail::TAsyncWriterImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TAsyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
