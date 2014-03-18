#pragma once

#include "public.h"

#include <core/misc/blob.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncReader
{
public:
    // Owns this fd
    explicit TAsyncReader(int fd);
    TAsyncReader(const TAsyncReader& other);
    ~TAsyncReader();

    // TODO(babenko): can't understand the meaning of this signature
    // WTF is pair<blob, bool> anyway?
    // need to change it or provide a comment at least
    std::pair<TBlob, bool> Read(TBlob&& buffer);
    TAsyncError GetReadyEvent();

    TError Abort();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
