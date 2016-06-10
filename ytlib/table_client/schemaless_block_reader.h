#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "unversioned_row.h"

#include <yt/core/yson/lexer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessBlockReader
    : public TNonCopyable
{
public:
    THorizontalSchemalessBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        const std::vector<int>& idMapping,
        int keyColumnCount,
        int extraColumnCount = 0);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKey(const TOwningKey& key);

    bool JumpToRowIndex(i64 rowIndex);

    const TOwningKey& GetKey() const;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool);

    i64 GetRowIndex() const;

private:
    TSharedRef Block_;
    NProto::TBlockMeta Meta_;

    // Maps chunk name table ids to client name table ids.
    std::vector<int> IdMapping_;
    const int KeyColumnCount_;
    // Count of extra row values, that are allocated and reserved
    // to be filled by upper levels (e.g. table_index).
    const int ExtraColumnCount_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    TOwningKey Key_;

    NYson::TStatelessLexer Lexer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
