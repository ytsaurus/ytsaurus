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
 *  If |true| is returned then some data was read synchronously.
 *  Call #GetReadLength to obtain its length.
 *  
 *  Otherwise no data could be read synchronously and an asynchronous request
 *  has been issued. Call #GetReadyEvent and subscribe to its return value to figure
 *  out when this request completes. Buffer passed to #Read must remain valid
 *  for the duration of the request. Upon request completion, call #GetReadLength
 *  (as above) to obtain the actual read length.
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
 *  If |true| is returned then the request is completed synchronously.
 *  
 *  Otherwise the data passed to #Write has been accepted but the request
 *  involves some asynchronous activities. Call #GetReadyEvent and subscribe
 *  to its return value to figure out when this request completes.
 *  Buffer passed to #Write must remain valid for the duration of the request.
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
