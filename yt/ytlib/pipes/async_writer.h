#pragma once

#include "public.h"

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriter
    : public NConcurrency::IAsyncOutputStream
{
public:
    // Owns this fd
    explicit TAsyncWriter(int fd);
    TAsyncWriter(const TAsyncWriter& other);
    ~TAsyncWriter();

    virtual TAsyncError Write(const void* data, size_t size) override;
    TAsyncError Close();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
