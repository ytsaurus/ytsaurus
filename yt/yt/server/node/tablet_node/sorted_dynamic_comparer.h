#pragma once

#include "public.h"
#include "dynamic_store_bits.h"

#include <yt/yt/ytlib/tablet_client/row_comparer_generator.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row);

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row, int prefix);

TRange<TUnversionedValue> ToKeyRef(TVersionedRow row);

//! Provides a comparer functor for dynamic row keys.
class TSortedDynamicRowKeyComparer
    : protected NTabletClient::TCGKeyComparers
{
public:
    TSortedDynamicRowKeyComparer() = default;

    TSortedDynamicRowKeyComparer(NTabletClient::TCGKeyComparers comparers);

    int operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const;
    int operator()(TSortedDynamicRow lhs, TRange<TUnversionedValue> rhs) const;
    int operator()(TRange<TUnversionedValue> lhs, TRange<TUnversionedValue> rhs) const;
    int operator()(TUnversionedRow lhs, TUnversionedRow rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
