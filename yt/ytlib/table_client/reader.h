#pragma once

#include "public.h"

#include <ytlib/table_client/chunk_meta.pb.h>

#include <core/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IReader
    : public virtual TRefCounted
{
    virtual TAsyncError Open(
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        bool includeAllColumns = false) = 0;

    //! Returns |true| while reading is in progress, |false| when reading is complete.
    //! If |rows->size() < rows->capacity()|, the client should wait for ready event before next call to #Read.
    //! Can throw, e.g. if some values in chunk are incompatible with schema.
    //! |rows| must be initially empty
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    // TODO(babenko): provide Read(std::vector<TUnversionedRow>* rows)

    virtual TAsyncError GetReadyEvent() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
