#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IWriter
    : public TRefCounted
{
    virtual void Open(
        TNameTablePtr nameTable,
        const NProto::TTableSchemaExt& schema,
        const TKeyColumns& keyColumns = TKeyColumns(),
        ERowsetType type = ERowsetType::Simple) = 0;

    virtual void WriteValue(const TRowValue& value) = 0;
    virtual bool EndRow(TTimestamp timestamp = NullTimestamp, bool deleted = false) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

    virtual TAsyncError AsyncClose() = 0;

    virtual i64 GetRowIndex() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
