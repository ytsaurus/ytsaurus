#pragma once

#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue DoMakeUnversionedValue(ui64 value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(i64 value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(double value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(TString value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(bool value, int columnId);

NTableClient::TVersionedValue DoMakeVersionedValue(
    ui64 value,
    NTableClient::TTimestamp timestamp,
    int columnnId,
    bool aggregate);

NTableClient::TVersionedValue DoMakeVersionedValue(
    i64 value,
    NTableClient::TTimestamp timestamp,
    int columnnId,
    bool aggregate);

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp timestamp);

template <class T>
void AppendVector(std::vector<T>* data, const std::vector<T> toAppend)
{
    data->insert(data->end(), toAppend.begin(), toAppend.end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat


