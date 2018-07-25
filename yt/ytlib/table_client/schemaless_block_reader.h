#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/yson/lexer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessBlockReader
    : public TNonCopyable
{
public:
    /*!
     *  For schemaless blocks id mapping must be of the chunk name table size.
     *  Reader ids are stored in ReadSchemaIndex of column mapping.
     *  If ReadSchemaIndex < 0, a column must be omitted.
     */

    THorizontalSchemalessBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        const std::vector<TColumnIdMapping>& idMapping,
        int chunkKeyColumnCount,
        int keyColumnCount,
        int extraColumnCount = 0);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKey(const TKey key);

    bool JumpToRowIndex(i64 rowIndex);

    TKey GetKey() const;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool);
    TMutableVersionedRow GetVersionedRow(TChunkedMemoryPool* memoryPool, TTimestamp timestamp);

    i64 GetRowIndex() const;

private:
    TSharedRef Block_;
    NProto::TBlockMeta Meta_;

    // Maps chunk name table ids to client name table ids.
    std::vector<TColumnIdMapping> IdMapping_;

    // If chunk key column count is smaller than key column count, key is extended with Nulls.
    // If chunk key column count is larger than key column count, key is trimmed.
    const int ChunkKeyColumnCount_;
    const int KeyColumnCount_;

    // Count of extra row values, that are allocated and reserved
    // to be filled by upper levels (e.g. table_index).
    const int ExtraColumnCount_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    const static size_t DefaultKeyBufferCapacity = 512;
    SmallVector<char, DefaultKeyBufferCapacity> KeyBuffer_;
    TMutableKey Key_;

    NYson::TStatelessLexer Lexer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
