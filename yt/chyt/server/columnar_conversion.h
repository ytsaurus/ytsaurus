#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <Core/Block.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::ColumnString::MutablePtr ConvertStringLikeYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint);
DB::MutableColumnPtr ConvertStringLikeYTColumnToLowCardinalityCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    bool insideOptional);
DB::MutableColumnPtr ConvertBooleanYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertDoubleYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertFloatYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertIntegerYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    NTableClient::ESimpleLogicalValueType type);
DB::MutableColumnPtr ConvertTzYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    NTableClient::ESimpleLogicalValueType type);
DB::MutableColumnPtr ConvertIntegerYTColumnToLowCardinalityCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    NTableClient::ESimpleLogicalValueType type,
    bool insideOptional);
DB::MutableColumnPtr ConvertNullYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::ColumnUInt8::MutablePtr BuildNullBytemapForCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::ColumnString::MutablePtr ConvertCHColumnToAny(
    const DB::IColumn& column,
    NTableClient::ESimpleLogicalValueType type,
    EExtendedYsonFormat ysonFormat = EExtendedYsonFormat::Binary);

// Reduces filter column to size of dictionary in dictionary or rle encoding,
// to filter unique values wihout materialization when distinct read optimization is used.
void ReduceFilterToDistinct(
    DB::IColumn::Filter& filter,
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
