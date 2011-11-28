#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include "../misc/ref_counted_base.h"
#include "../misc/async_stream_state.h"
#include "../misc/sync.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IAsyncWriter> Ptr;

    // TODO: -> AsyncOpen
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncInit() = 0;

    virtual void Write(const TColumn& column, TValue value) = 0;

    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow() = 0;
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncClose() = 0;

    // TODO: TError
    virtual void Cancel(const Stroka& errorMessage) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ISyncWriter> TPtr;

    // TODO: -> Open
    virtual void Init() = 0;
    virtual void Write(const TColumn& column, TValue value) = 0;
    virtual void EndRow() = 0;
    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriter
    : public IAsyncWriter
    , public ISyncWriter
{
    void Init()
    {
        Sync<IAsyncWriter>(this, &IAsyncWriter::AsyncInit);
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
