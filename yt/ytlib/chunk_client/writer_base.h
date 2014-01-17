#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/ref_counted.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Purpose of this interface is to be a virtual base for template TMultiChunkSequentialWriter
//! and particular writers, e.g. IVersionedWriter, to mix them up.
struct IWriterBase
    : public virtual TRefCounted
{
    virtual TAsyncError Open() = 0;
    virtual TAsyncError GetReadyEvent() = 0;
    virtual TAsyncError Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT