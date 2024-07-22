#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/versioned_block_reader.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TSlimVersionedBlockReader
    : public NTableClient::TVersionedRowParserBase
{
public:
    static constexpr NChunkClient::EChunkFormat ChunkFormat = NChunkClient::EChunkFormat::TableVersionedSlim;

    TSlimVersionedBlockReader(
        TSharedRef block,
        const NTableClient::NProto::TDataBlockMeta& blockMeta,
        int blockFormatVersion,
        const NTableClient::TTableSchemaPtr& chunkSchema,
        int keyColumnCount,
        const std::vector<NTableClient::TColumnIdMapping>& schemaIdMapping,
        const NTableClient::TKeyComparer& keyComparer,
        NTableClient::TTimestamp timestamp,
        bool produceAllVersions);

    NTableClient::TLegacyKey GetKey() const;
    NTableClient::TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

    bool NextRow();
    bool SkipToRowIndex(int rowIndex);
    bool SkipToKey(NTableClient::TLegacyKey key);

private:
    const TSharedRef Block_;
    const NTableClient::TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const int KeyColumnCount_;
    const int ChunkKeyColumnCount_;
    // NB: Chunk reader holds the comparer.
    const NTableClient::TKeyComparer& KeyComparer_;
    const int RowCount_;
    const int ReaderSchemaColumnCount_;

    int ValueCountPerRowEstimate_;

    TCompactVector<bool, NTableClient::TypicalColumnCount> ReaderIdToAggregateFlagStorage_;
    bool* const ReaderIdToAggregateFlag_;

    TCompactVector<int, NTableClient::TypicalColumnCount> ChunkToReaderIdMappingStorage_;
    int* const ChunkToReaderIdMapping_;

    TCompactVector<int, NTableClient::TypicalColumnCount> ReaderToChunkIdMappingStorage_;
    int* const ReaderToChunkIdMapping_;

    TCompactVector<NTableClient::TTimestamp, NTableClient::TypicalColumnCount> DeleteTimestampStorage_;

    struct TDictionary
    {
        const ui32* Offsets;
        const char* Data;
    };

    const ui32* RowOffsets_;
    const NTableClient::TTimestamp* Timestamps_;
    TDictionary KeyDictionary_;
    TDictionary ValueDictionary_;
    const char* Rows_;

    bool Valid_ = false;
    int RowIndex_ = -1;
    const char* RowPtr_ = nullptr;

    static constexpr int DefaultKeyScratchCapacity = 128;
    TCompactVector<char, DefaultKeyScratchCapacity> KeyScratch_;
    NTableClient::TLegacyMutableKey Key_;

    static constexpr int DefaultDeleteTimestampScratchCapacity = 16;
    TCompactVector<NTableClient::TTimestamp, DefaultDeleteTimestampScratchCapacity> DeleteTimestampScratch_;

    static constexpr int DefaultWriteTimestampScratchCapacity = 64;
    TCompactVector<NTableClient::TTimestamp, DefaultWriteTimestampScratchCapacity> WriteTimestampScratch_;

    static constexpr int DefaultValueScratchCapacity = 64;
    TCompactVector<NTableClient::TVersionedValue, DefaultValueScratchCapacity> ValueScratch_;

    bool JumpToRowIndex(int rowIndex);

    void FillKey(NTableClient::TMutableVersionedRow row);

    template <class TValueParser>
    void ParseValues(
        TDictionary dictionary,
        const char*& ptr,
        TValueParser valueParser);

    template <class TValueCtor>
    void ReadValues(
        TDictionary dictionary,
        const char*& ptr,
        TValueCtor valueCtor);
    void SkipValues(
        TDictionary dictionary,
        const char*& ptr);

    void ReadNonNullValue(
        const char*& ptr,
        int chunkSchemaId,
        NTableClient::TUnversionedValue* value);
    void SkipNonNullValue(
        const char*& ptr,
        int chunkSchemaId);

    NTableClient::TMutableVersionedRow ReadRowAllVersions(TChunkedMemoryPool* memoryPool);
    NTableClient::TMutableVersionedRow ReadRowSingleVersion(TChunkedMemoryPool* memoryPool);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
