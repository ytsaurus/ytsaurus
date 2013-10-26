#pragma once

#include "public.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

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
        bool includeAllColumns = false,
        ERowsetType type = ERowsetType::Simple) = 0;

    //! Returns |true| while reading is in progress, |false| when reading is complete.
    //! If |rows->size() < rows->capacity()|, the client should wait for ready event before next call to #Read.
    //! Can throw, e.g. if some values in chunk are incompatible with schema.
    //! |rows| must be initially empty
    virtual bool Read(std::vector<TRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
