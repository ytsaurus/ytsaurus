#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"
#include "row_comparer_generator.h"

#include <core/misc/enum.h>
#include <core/misc/chunked_memory_pool.h>
#include <core/misc/intrusive_ptr.h>

#include <ytlib/table_client/unversioned_row.h>

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

struct TRowWrapper
{
    TUnversionedRow Row;
};

struct TKeyWrapper
{
    TUnversionedRow Row;
};

//! Provides a comparer functor for dynamic row keys.
class TDynamicRowKeyComparer
{
public:
    static TDynamicRowKeyComparer Create(
        int keyColumnCount,
        const TTableSchema& schema);

    TDynamicRowKeyComparer() = default;

    int operator()(TDynamicRow lhs, TDynamicRow rhs) const;
    int operator()(TDynamicRow lhs, TRowWrapper rhs) const;
    int operator()(TDynamicRow lhs, TKeyWrapper rhs) const;
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

    TDynamicRowKeyComparer(
        int keyColumnCount,
        NCodegen::TCGFunction<TDDComparerSignature> ddComparer,
        NCodegen::TCGFunction<TDUComparerSignature> duComparer,
        NCodegen::TCGFunction<TUUComparerSignature> uuComparer);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
