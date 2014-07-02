#pragma once

#include "public.h"

#include <ytlib/chunk_client/data_statistics.h>

#include <core/misc/error.h>
#include <core/misc/ref_counted.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IWriterBase
    : public virtual TRefCounted
{
    virtual void WriteRow(const TRow& row) = 0;

    virtual i64 GetRowCount() const = 0;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public IWriterBase
{
    virtual void Open() = 0;

    virtual bool IsReady() = 0;

    virtual TAsyncError GetReadyEvent() = 0;

    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
