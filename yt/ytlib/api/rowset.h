#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <ytlib/table_client/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const NTableClient::TTableSchema& GetSchema() const = 0;
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;
    virtual const std::vector<NTableClient::TUnversionedRow>& GetRows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowset)

///////////////////////////////////////////////////////////////////////////////

IRowsetPtr CreateRowset(
    std::vector<std::unique_ptr<NTabletClient::TWireProtocolReader>> readers,
    const NTableClient::TTableSchema& schema,
    std::vector<NTableClient::TUnversionedRow> rows);

std::tuple<NTableClient::ISchemafulWriterPtr, TFuture<IRowsetPtr>>
    CreateSchemafulRowsetWriter(const NTableClient::TTableSchema& schema);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

