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
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertBooleanYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertDoubleYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertFloatYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::MutableColumnPtr ConvertIntegerYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn,
    NTableClient::ESimpleLogicalValueType type);
DB::MutableColumnPtr ConvertNullYTColumnToCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::ColumnUInt8::MutablePtr BuildNullBytemapForCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::ColumnString::MutablePtr ConvertCHColumnToAny(
    const DB::IColumn& column,
    NTableClient::ESimpleLogicalValueType type,
    EExtendedYsonFormat ysonFormat = EExtendedYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
