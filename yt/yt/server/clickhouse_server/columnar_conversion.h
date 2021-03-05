#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <Core/Block.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::MutableColumnPtr ConvertStringLikeYTColumnToCHColumn(
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
DB::ColumnUInt8::MutablePtr BuildNullBytemapForCHColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& ytColumn);
DB::ColumnString::MutablePtr ConvertCHColumnToAny(
    const DB::IColumn& column,
    NTableClient::ESimpleLogicalValueType type,
    NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
