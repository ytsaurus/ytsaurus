#pragma once

#include "public.h"
#include "dynamic_store_bits.h"
#include "row_comparer_generator.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/enum.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

/*
 * Row comparer can work with data rows and data keys.
 * Both of the latter are internally represented by TUnversionedRow.
 * However, the comparison semantics is different: data rows must contain
 * |keyColumnCount| key components at the very beginning (and the rest
 * is value components, which must be ignored) while data keys may be
 * of arbitrary size.
 *
 * To discriminate between data rows and data keys, we provide a pair of
 * wrappers on top of TUnversionedRow.
 */

struct TUnversionedRowWrapper
{
    TUnversionedRow Row;
};

struct TVersionedRowWrapper
{
    TVersionedRow Row;
};

struct TKeyWrapper
{
    TUnversionedRow Row;
};

//! Provides a comparer functor for dynamic row keys.
class TSortedDynamicRowKeyComparer
{
public:
    static TSortedDynamicRowKeyComparer Create(
        int keyColumnCount,
        const TTableSchema& schema);

    TSortedDynamicRowKeyComparer() = default;

    int operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const;
    int operator()(TSortedDynamicRow lhs, TUnversionedRowWrapper rhs) const;
    int operator()(TSortedDynamicRow lhs, TVersionedRowWrapper rhs) const;
    int operator()(TSortedDynamicRow lhs, TKeyWrapper rhs) const;
    int operator()(TUnversionedRow lhs, TUnversionedRow rhs) const;
    int operator()(
        const TUnversionedValue* lhsBegin,
        const TUnversionedValue* lhsEnd,
        const TUnversionedValue* rhsBegin,
        const TUnversionedValue* rhsEnd) const;

private:
    int KeyColumnCount_;
    NCodegen::TCGFunction<TDDComparerSignature> DDComparer_;
    NCodegen::TCGFunction<TDUComparerSignature> DUComparer_;
    NCodegen::TCGFunction<TUUComparerSignature> UUComparer_;

    TSortedDynamicRowKeyComparer(
        int keyColumnCount,
        NCodegen::TCGFunction<TDDComparerSignature> ddComparer,
        NCodegen::TCGFunction<TDUComparerSignature> duComparer,
        NCodegen::TCGFunction<TUUComparerSignature> uuComparer);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
