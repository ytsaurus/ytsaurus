#pragma once

#include "dynamic_store_bits.h"

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/codegen/function.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

typedef int(TDDComparerSignature)(ui32, const TDynamicValueData*, ui32, const TDynamicValueData*);
typedef int(TDUComparerSignature)(ui32, const TDynamicValueData*, const TUnversionedValue*, int);
typedef int(TUUComparerSignature)(const TUnversionedValue*, i32, const TUnversionedValue*, i32);

////////////////////////////////////////////////////////////////////////////////

std::tuple<
    NCodegen::TCGFunction<TDDComparerSignature>,
    NCodegen::TCGFunction<TDUComparerSignature>,
    NCodegen::TCGFunction<TUUComparerSignature>>
GenerateComparers(int keyColumnCount, const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
