#pragma once

#include "public.h"

#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/misc/error.h>
#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IWriterBase
    : public virtual TRefCounted
{
    virtual void WriteRow(const TRow& row) = 0;

    virtual i64 GetRowCount() const = 0;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public IWriterBase
{
    virtual TAsyncError AsyncOpen() = 0;

    virtual bool IsReady() = 0;

    virtual TAsyncError GetReadyEvent() = 0;

    virtual TAsyncError AsyncClose() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
