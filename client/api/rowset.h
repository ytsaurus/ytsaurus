#pragma once

#include "public.h"

#include <yt/client/table_client/public.h>

#include <yt/client/tablet_client/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct IRowset
    : public virtual TRefCounted
{
    virtual const NTableClient::TTableSchema& Schema() const = 0;

    virtual TRange<TRow> GetRows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedRowset)
DEFINE_REFCOUNTED_TYPE(IVersionedRowset)

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowsetPtr CreateRowset(
    const NTableClient::TTableSchema& schema,
    TSharedRange<NTableClient::TUnversionedRow> rows);

IVersionedRowsetPtr CreateRowset(
    const NTableClient::TTableSchema& schema,
    TSharedRange<NTableClient::TVersionedRow> rows);

std::tuple<NTableClient::ISchemafulWriterPtr, TFuture<IUnversionedRowsetPtr>>
    CreateSchemafulRowsetWriter(const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

