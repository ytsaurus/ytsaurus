#pragma once

#include "public.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IWriter
    : public virtual TRefCounted
{
    virtual void Open(
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns = TKeyColumns()) = 0;

    virtual void WriteValue(const TUnversionedValue& value) = 0;

    virtual bool EndRow() = 0;

    virtual TFuture<void> GetReadyEvent() = 0;

    virtual TFuture<void> Close() = 0;

    virtual i64 GetRowIndex() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
