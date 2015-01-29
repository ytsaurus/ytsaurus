#pragma once

#include "dynamic_memory_store_bits.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>

#include <core/codegen/function.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

typedef int(TDDComparerSignature)(ui32, const TDynamicValueData*, ui32, const TDynamicValueData*);
typedef int(TDUComparerSignature)(ui32, const TDynamicValueData*, const TUnversionedValue*, int);
typedef int(TUUComparerSignature)(const TUnversionedValue*, const TUnversionedValue*);

////////////////////////////////////////////////////////////////////////////////

std::tuple<
    NCodegen::TCGFunction<TDDComparerSignature>,
    NCodegen::TCGFunction<TDUComparerSignature>,
    NCodegen::TCGFunction<TUUComparerSignature>>
GenerateComparers(int keyColumnCount, const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
