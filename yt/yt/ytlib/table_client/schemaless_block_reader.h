#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/lexer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

std::vector<bool> GetCompositeColumnFlags(const TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

class THorizontalBlockReader
    : public TNonCopyable
{
public:
    /*!
     *  For schemaless blocks id mapping must be of the chunk name table size.
     *  It maps chunk ids to reader ids.
     *  Column is omitted if reader id < 0.
     */

    THorizontalBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& meta,
        const std::vector<bool>& compositeColumnFlags,
        const std::vector<int>& chunkToReaderIdMapping,
        int chunkComparatorLength,
        const TComparator& comparator,
        int extraColumnCount = 0);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKeyBound(const TKeyBound& lowerBound);
    bool SkipToKey(const TLegacyKey key);

    bool JumpToRowIndex(i64 rowIndex);

    TLegacyKey GetLegacyKey() const;
    TKey GetKey() const;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool);
    TMutableVersionedRow GetVersionedRow(TChunkedMemoryPool* memoryPool, TTimestamp timestamp);

    i64 GetRowIndex() const;

private:
    const TSharedRef Block_;
    const NProto::TDataBlockMeta Meta_;

    // Maps chunk name table ids to client name table ids.
    std::vector<int> ChunkToReaderIdMapping_;

    std::vector<bool> CompositeColumnFlags_;

    // If chunk key column count is smaller than key column count, key is extended with Nulls.
    // If chunk key column count is larger than key column count, key is trimmed.
    const int ChunkComparatorLength_;
    const TComparator Comparator_;

    // Count of extra row values, that are allocated and reserved
    // to be filled by upper levels (e.g. table_index).
    const int ExtraColumnCount_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    constexpr static size_t DefaultKeyBufferCapacity = 128;

    TCompactVector<char, DefaultKeyBufferCapacity> KeyBuffer_;
    TMutableUnversionedRow Key_;

    NYson::TStatelessLexer Lexer_;

    TUnversionedValue TransformAnyValue(TUnversionedValue value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
