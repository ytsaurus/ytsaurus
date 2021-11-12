#pragma once

#include "public.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/tablet_client/dynamic_value.h>

#include <yt/yt/library/codegen/function.h>

#include <yt/yt/core/misc/sync_cache.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletClient {

using NTableClient::EValueType;
using NTableClient::TUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

typedef int(TDDComparerSignature)(ui32, const TDynamicValueData*, ui32, const TDynamicValueData*);
typedef int(TDUComparerSignature)(ui32, const TDynamicValueData*, const TUnversionedValue*, int);
typedef int(TUUComparerSignature)(const TUnversionedValue*, i32, const TUnversionedValue*, i32);

struct TCGKeyComparers
{
    NCodegen::TCGFunction<TDDComparerSignature> DDComparer;
    NCodegen::TCGFunction<TDUComparerSignature> DUComparer;
    NCodegen::TCGFunction<TUUComparerSignature> UUComparer;

    int operator()(NTableClient::TUnversionedRow lhs, NTableClient::TUnversionedRow rhs) const
    {
        return UUComparer(lhs.Begin(), lhs.GetCount(), rhs.Begin(), rhs.GetCount());
    }
};

////////////////////////////////////////////////////////////////////////////////

TCGKeyComparers GenerateComparers(TRange<EValueType> keyColumnTypes);

////////////////////////////////////////////////////////////////////////////////

struct IRowComparerProvider
    : public virtual TRefCounted
{
    virtual TCGKeyComparers Get(NTableClient::TKeyColumnTypes keyColumnTypes) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowComparerProvider)

IRowComparerProviderPtr CreateRowComparerProvider(TSlruCacheConfigPtr config);

} // namespace NYT::NTabletClient
