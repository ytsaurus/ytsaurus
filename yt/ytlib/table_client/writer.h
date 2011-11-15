#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include "../misc/ref_counted_base.h"
#include "../misc/async_stream_state.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCountedBase
    , public ISyncInterface
{
    typedef TIntrusivePtr<IAsyncWriter> Ptr;

    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncInit() = 0;

    virtual void Write(const TColumn& column, TValue value) = 0;

    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow() = 0;
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncClose() = 0;

    virtual void Cancel(const Stroka& errorMessage) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ISyncWriter> TPtr;

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
        Sync(&IAsyncWriter::AsyncInit);
    }

    void EndRow()
    {
        Sync(&IAsyncWriter::AsyncEndRow);
    }

    void Close()
    {
        Sync(&IAsyncWriter::AsyncClose);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
