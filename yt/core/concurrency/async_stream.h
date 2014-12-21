#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/misc/error.h>
#include <core/misc/ref.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for reading from a stream.
struct IAsyncInputStream
    : public virtual TRefCounted
{
    //! Starts reading another block of data.
    /*!
     *  Call #Read and provide a buffer to start reading.
     *  Buffer passed to #Read must remain valid until the returned future is set.
     *  One must not call #Read again before the previous call is complete.
     *  Returns number of bytes read or an error.
     */
    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncInputStream)

//! Creates a synchronous adapter from a given asynchronous stream.
std::unique_ptr<TInputStream> CreateSyncAdapter(IAsyncInputStreamPtr underlyingStream);

//! Creates an asynchronous adapter from a given synchronous stream.
IAsyncInputStreamPtr CreateAsyncAdapter(TInputStream* underlyingStream);

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for writing to a stream.
struct IAsyncOutputStream
    : public virtual TRefCounted
{
    //! Starts writing another block of data.
    /*!
     *  Call #Write to issue a write request.
     *  Buffer passed to #Write must remain valid until the returned future is set.
     *  One must not call #Write again before the previous call is complete.
     */
    virtual TAsyncError Write(const void* buf, size_t len) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncOutputStream)

//! Creates a synchronous adapter from a given asynchronous stream.
std::unique_ptr<TOutputStream> CreateSyncAdapter(IAsyncOutputStreamPtr underlyingStream);

//! Creates an asynchronous adapter from a given synchronous stream.
IAsyncOutputStreamPtr CreateAsyncAdapter(TOutputStream* underlyingStream);

////////////////////////////////////////////////////////////////////////////////

//! Similar to IAsyncInputStream but is essentially zero-copy, i.e.
//! produces a sequence of memory blocks with shared ownership.
struct IAsyncZeroCopyInputStream
    : public virtual TRefCounted
{
    //! Requests another block of data.
    /*!
     *  Returns the data or an error.
     *  If a null TSharedRef is returned then end-of-stream is reached.
     *  One must not call #Read again before the previous call is complete.
     *  A sane implementation must guarantee that it returns blocks of sensible size.
     */
    virtual TFuture<TErrorOr<TSharedRef>> Read() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyInputStream)

//! Creates a zero-copy adapter from a given asynchronous stream.
IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize = 64 * 1024);

//! Creates a copying adapter from a given asynchronous zero-copy stream.
IAsyncInputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream);

////////////////////////////////////////////////////////////////////////////////

//! Similar to IAsyncOutputStream but is essentially zero-copy, i.e.
//! produces a sequence of memory blocks with shared ownership.
struct IAsyncZeroCopyOutputStream
    : public virtual TRefCounted
{
    //! Requests another block of data.
    /*!
     *  Returns an error, if any.
     *  In contrast to IAsyncOutputStream, one may call #Write again before
     *  the previous call is complete.
     */
    virtual TAsyncError Write(const TSharedRef& data) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyOutputStream)

//! Creates a zero-copy adapter from a given asynchronous stream.
IAsyncZeroCopyOutputStreamPtr CreateZeroCopyAdapter(IAsyncOutputStreamPtr underlyingStream);

//! Creates a copying adapter from a given asynchronous zero-copy stream.
IAsyncOutputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream);

//! Creates an adapter that prefetches data in background.
/*!
 *  The adapter tries to maintain up to #windowSize bytes of data by
 *  retrieving blocks from #underlyingStream in background.
 */
IAsyncZeroCopyInputStreamPtr CreatePrefetchingAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    size_t windowSize);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
