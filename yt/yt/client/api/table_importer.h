#pragma once

#include "public.h"
#include "row_batch_writer.h"

namespace NYT::NApi {

struct ITableImporter
    : public virtual TRefCounted
{
    //! Returns the schema to be used for constructing rows.
    virtual const NTableClient::TTableSchemaPtr& GetSchema() const = 0;

    //! Closes the importer. Must be the last call to the importer.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableImporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
