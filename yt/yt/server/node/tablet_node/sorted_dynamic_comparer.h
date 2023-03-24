#pragma once

#include "public.h"
#include "dynamic_store_bits.h"

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Provides a comparer functor for dynamic row keys.
class TSortedDynamicRowKeyComparer
    : public NQueryClient::TCGKeyComparers
{
public:
    TSortedDynamicRowKeyComparer() = default;

    TSortedDynamicRowKeyComparer(NQueryClient::TCGKeyComparers comparers);

    int operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const;
    int operator()(TSortedDynamicRow lhs, TUnversionedValueRange rhs) const;
    int operator()(TUnversionedValueRange lhs, TSortedDynamicRow rhs) const;
    int operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const;
    int operator()(TUnversionedRow lhs, TUnversionedRow rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
