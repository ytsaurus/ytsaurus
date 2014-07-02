#pragma once

#include "public.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriter
{
public:
    // Owns this fd
    explicit TAsyncWriter(int fd);
    TAsyncWriter(const TAsyncWriter& other);
    ~TAsyncWriter();

    bool Write(const void* data, size_t size);
    TAsyncError AsyncClose();
    TAsyncError GetReadyEvent();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
