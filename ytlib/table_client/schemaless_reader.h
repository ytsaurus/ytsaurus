#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/reader_base.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessReader
    : public virtual NChunkClient::IReaderBase
{
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    virtual const TNameTablePtr& GetNameTable() const = 0;

    virtual TKeyColumns GetKeyColumns() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
