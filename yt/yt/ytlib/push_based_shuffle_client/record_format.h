#pragma once

#include <yt/yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/memory/range.h>
#include <library/cpp/yt/memory/ref.h>

#include <optional>
#include <vector>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

//! 16-byte fixed POD header preceding each wire shuffle record's payload.
struct TRecordHeader
{
    i32 RowCount;
    i32 MapperId;
    i64 StartRow;
};

static_assert(sizeof(TRecordHeader) == 16, "sizeof(TRecordHeader) != 16");

////////////////////////////////////////////////////////////////////////////////

//! Header + uncompressed payload. UncompressedPayload is the row data
//! only (offset table + varint-encoded rows); the 16-byte Header is a
//! separate field and is never serialized into UncompressedPayload.
struct TShuffleRecord
{
    TRecordHeader Header;
    std::vector<TSharedRef> UncompressedPayload;
};

//! String-typed values in Rows reference bytes inside UncompressedPayload.
//! Callers must keep UncompressedPayload alive while iterating Rows.
struct TParsedRecord
{
    TRecordHeader Header;
    TSharedRef UncompressedPayload;
    TRange<NTableClient::TUnversionedRow> Rows;
};

////////////////////////////////////////////////////////////////////////////////

//! Accumulates rows from one mapper and emits a TShuffleRecord per flush.
class TShuffleRecordBuilder
{
public:
    TShuffleRecordBuilder(i32 mapperId, i64 startRowId);

    void AddRow(NTableClient::TUnversionedRow row);

    //! Returns the buffered rows as a TShuffleRecord, or nullopt if no
    //! rows are buffered.
    std::optional<TShuffleRecord> FlushRecord();

    //! Bytes currently allocated for buffered row data.
    i64 GetAllocatedDataSize() const;

private:
    const i32 MapperId_;
    i64 NextRowId_;
    //! Has a 64 KiB+1 minimum reserve floor (a v1 limitation).
    std::optional<NTableClient::THorizontalBlockWriter> BlockWriter_;
};

////////////////////////////////////////////////////////////////////////////////

//! Reads the 16-byte header from the front of a wire shuffle record.
TRecordHeader ReadShuffleRecordHeader(TRange<TSharedRef> wire);

std::vector<TSharedRef> CompressShuffleRecord(
    const TShuffleRecord& record,
    NCompression::ECodec codec);

TShuffleRecord DecompressShuffleRecord(
    TRange<TSharedRef> wire,
    NCompression::ECodec codec);

//! Materializes the rows of a shuffle record.
TParsedRecord ParseShuffleRecord(
    TShuffleRecord record,
    TChunkedMemoryPool* pool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
