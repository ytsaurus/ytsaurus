#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TSlimVersionedBlockWriter
    : public NTableClient::TVersionedBlockWriterBase
{
public:
    TSlimVersionedBlockWriter(
        NTableClient::TSlimVersionedWriterConfigPtr config,
        NTableClient::TTableSchemaPtr schema,
        TMemoryUsageTrackerGuard guard = {});
    ~TSlimVersionedBlockWriter();

    void WriteRow(NTableClient::TVersionedRow row);

    NTableClient::TBlock FlushBlock();

    i64 GetBlockSize() const;

private:
    class TDictionary;
    class TValueFrequencyCollector;

    TCompactVector<NTableClient::ESimpleLogicalValueType, NTableClient::TypicalColumnCount> LogicalColumnTypes_;

    THashMap<NTableClient::TTimestamp, int> TimestampToIndex_;
    TChunkedOutputStream TimestampDataStream_;

    std::vector<int> RowBufferOffsets_;
    TChunkedOutputStream RowBufferStream_;
    TChunkedMemoryPool StringPool_;

    TChunkedOutputStream RowOffsetStream_;
    TChunkedOutputStream RowDataStream_;

    TChunkedOutputStream KeyDictionaryOffsetStream_;
    TChunkedOutputStream KeyDictionaryDataStream_;

    TChunkedOutputStream ValueDictionaryOffsetStream_;
    TChunkedOutputStream ValueDictionaryDataStream_;

    std::vector<std::unique_ptr<TValueFrequencyCollector>> ValueFrequencyCollectors_;

    int ValueCount_ = 0;

    NTableClient::TUnversionedValue CaptureValue(NTableClient::TUnversionedValue value);
    int GetTimestampIndex(NTableClient::TTimestamp timestamp);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
