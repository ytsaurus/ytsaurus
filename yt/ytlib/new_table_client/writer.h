#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

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

} // namespace NVersionedTableClient
} // namespace NYT
