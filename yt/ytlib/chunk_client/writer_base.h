#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! The purpose of this interface is to be a virtual base for TMultiChunkSequentialWriter
//! and some specific writers, e.g. IVersionedWriter, to mix them up.
struct IWriterBase
    : public virtual TRefCounted
{
    //! Initializes the writer. Must be called (and its result must be waited for)
    //! before making any other calls.
    virtual TFuture<void> Open() = 0;

    //! Returns an asynchronous flag enabling to wait until data is written.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Closes the writer. Must be the last call to the writer.
    virtual TFuture<void> Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
