#pragma once

#include "public.h"

#include <yt/client/chunk_client/reader_base.h>

#include <yt/core/actions/future.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowBatchReadOptions
{
    //! The desired number of rows to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxRowsPerRead = 10000;

    //! The desired data weight to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxDataWeightPerRead = 16_MB;

    //! If true then the reader may return a columnar batch.
    //! If false then the reader must return a non-columnar batch.
    bool Columnar = false;
};

struct IUnversionedReaderBase
    : public virtual NChunkClient::IReaderBase
{
    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchemafulUnversionedReader
    : public IUnversionedReaderBase
{ };

DEFINE_REFCOUNTED_TYPE(ISchemafulUnversionedReader)

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessUnversionedReader
    : public IUnversionedReaderBase
{
    virtual const TNameTablePtr& GetNameTable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessUnversionedReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
