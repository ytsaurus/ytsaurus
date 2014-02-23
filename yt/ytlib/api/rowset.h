#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const NVersionedTableClient::TTableSchema& Schema() const = 0;
    virtual const std::vector<NVersionedTableClient::TUnversionedRow>& Rows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowset)

IRowsetPtr CreateRowset(
    std::vector<std::unique_ptr<NTabletClient::TWireProtocolReader>> readers,
    const NVersionedTableClient::TTableSchema& schema,
    std::vector<NVersionedTableClient::TUnversionedRow> rows);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

