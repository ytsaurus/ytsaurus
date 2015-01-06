#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const NVersionedTableClient::TTableSchema& GetSchema() const = 0;
    virtual const NVersionedTableClient::TNameTablePtr& GetNameTable() const = 0;
    virtual const std::vector<NVersionedTableClient::TUnversionedRow>& GetRows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowset)

///////////////////////////////////////////////////////////////////////////////

IRowsetPtr CreateRowset(
    std::vector<std::unique_ptr<NTabletClient::TWireProtocolReader>> readers,
    const NVersionedTableClient::TTableSchema& schema,
    std::vector<NVersionedTableClient::TUnversionedRow> rows);

std::tuple<NVersionedTableClient::ISchemafulWriterPtr, TPromise<IRowsetPtr>>
    CreateSchemafulRowsetWriter();

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

