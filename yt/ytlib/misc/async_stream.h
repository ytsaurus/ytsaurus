#pragma once

#include "error.h"
#include "sync.h"
#include "intrusive_ptr.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncInputStream
    : public virtual TRefCounted
{
    //! Returns true if read operation is done.
    //! Otherwise you should wait GetReadFuture().
    virtual bool Read(void* buf, size_t len) = 0;
    virtual TAsyncError GetReadFuture() = 0;
    virtual size_t GetReadLength() const = 0;
};

typedef TIntrusivePtr<IAsyncInputStream> IAsyncInputStreamPtr;

std::unique_ptr<TInputStream> CreateSyncInputStream(IAsyncInputStreamPtr asyncStream);

IAsyncInputStreamPtr CreateAsyncInputStream(TInputStream* syncStream);

////////////////////////////////////////////////////////////////////////////////

struct IAsyncOutputStream
    : public virtual TRefCounted
{
    //! Returns true if write operation is done. 
    //! Otherwise you should wait GetWriteFuture().
    virtual bool Write(const void* buf, size_t len) = 0;
    virtual TAsyncError GetWriteFuture() = 0;
};

typedef TIntrusivePtr<IAsyncOutputStream> IAsyncOutputStreamPtr;

std::unique_ptr<TOutputStream> CreateSyncOutputStream(IAsyncOutputStreamPtr asyncStream);

IAsyncOutputStreamPtr CreateAsyncOutputStream(TOutputStream* asyncStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
