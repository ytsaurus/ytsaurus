#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/misc/sync.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for reading from a stream.
/*!
 *  Call #Read and provide a buffer to start reading.
 *  Buffer passed to #Read must remain valid until the returned future is set. 
 *  Returns number of bytes read or error.
 *
 */
struct IAsyncInputStream
    : public virtual TRefCounted
{
    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncInputStream)

std::unique_ptr<TInputStream> CreateSyncInputStream(IAsyncInputStreamPtr asyncStream);
IAsyncInputStreamPtr CreateAsyncInputStream(TInputStream* syncStream);

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for writing to a stream.
/*!
 *  Call #Write to issue a write request.
 *  Buffer passed to #Write must remain valid until the retured future is set.
 *
 */
struct IAsyncOutputStream
    : public virtual TRefCounted
{
    virtual TAsyncError Write(const void* buf, size_t len) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncOutputStream)

std::unique_ptr<TOutputStream> CreateSyncOutputStream(IAsyncOutputStreamPtr asyncStream);
IAsyncOutputStreamPtr CreateAsyncOutputStream(TOutputStream* asyncStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
