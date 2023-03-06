#pragma once

#include "public.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/tablet_client/dynamic_value.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletClient {

using NTableClient::EValueType;
using NTableClient::TUnversionedValue;
using NTableClient::TUnversionedRow;

////////////////////////////////////////////////////////////////////////////////

typedef int(TDDComparerSignature)(ui32, const TDynamicValueData*, ui32, const TDynamicValueData*);
typedef int(TDUComparerSignature)(ui32, const TDynamicValueData*, const TUnversionedValue*, int);
typedef int(TUUComparerSignature)(const TUnversionedValue*, const TUnversionedValue*, i32);

struct TCGKeyComparers
{
    TCallback<TDDComparerSignature> DDComparer;
    TCallback<TDUComparerSignature> DUComparer;
    TCallback<TUUComparerSignature> UUComparer;
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
