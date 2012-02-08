#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IAsyncWriter> Ptr;

    virtual TAsyncError::TPtr AsyncOpen() = 0;

    virtual void Write(const TColumn& column, TValue value) = 0;

    virtual TAsyncError::TPtr AsyncEndRow() = 0;
    virtual TAsyncError::TPtr AsyncClose() = 0;

    virtual void Cancel(const TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ISyncWriter> TPtr;

    virtual void Open() = 0;
    virtual void Write(const TColumn& column, TValue value) = 0;
    virtual void EndRow() = 0;
    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriter
    : public IAsyncWriter
    , public ISyncWriter
{
    void Open()
    {
        Sync<IAsyncWriter>(this, &IAsyncWriter::AsyncOpen);
    }

    void EndRow()
    {
        Sync<IAsyncWriter>(this, &IAsyncWriter::AsyncEndRow);
    }

    void Close()
    {
        Sync<IAsyncWriter>(this, &IAsyncWriter::AsyncClose);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
