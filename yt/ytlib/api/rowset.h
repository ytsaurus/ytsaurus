#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const NTableClient::TTableSchema& Schema() const = 0;
    virtual TRange<NTableClient::TUnversionedRow> GetRows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowset)

///////////////////////////////////////////////////////////////////////////////

IRowsetPtr CreateRowset(
    const NTableClient::TTableSchema& schema,
    TSharedRange<NTableClient::TUnversionedRow> rows);

std::tuple<NTableClient::ISchemafulWriterPtr, TFuture<IRowsetPtr>>
    CreateSchemafulRowsetWriter(const NTableClient::TTableSchema& schema);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

