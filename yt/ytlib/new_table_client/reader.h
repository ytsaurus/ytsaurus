#pragma once

#include "public.h"
#include "row.h"
#include "schema.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IReader
    : public virtual TRefCounted
{
    virtual TAsyncError Open(
        TNameTablePtr nameTable, 
        const TTableSchema& schema, 
        bool includeAllColumns = true,
        ERowsetType type = ERowsetType::Simple) = 0;

    virtual bool Read(std::vector<TRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
