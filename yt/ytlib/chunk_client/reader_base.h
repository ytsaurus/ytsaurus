#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/ref_counted.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderBase
    : public virtual TRefCounted
{
    virtual TAsyncError Open() = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
