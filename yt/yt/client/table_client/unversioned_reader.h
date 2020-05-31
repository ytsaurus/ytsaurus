#pragma once

#include "public.h"

#include <yt/client/chunk_client/reader_base.h>

#include <yt/core/actions/future.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowBatchReadOptions
{
    i64 MaxRowsPerRead = 10000;
    i64 MaxDataWeightPerRead = 16_MB;
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
    virtual const TKeyColumns& GetKeyColumns() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessUnversionedReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
