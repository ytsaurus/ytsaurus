#pragma once

#include "public.h"

#include <core/concurrency/async_stream.h>

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

    virtual ~TAsyncWriter();

    int GetHandle() const;

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
