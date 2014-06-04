#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"
#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessBlockReader
    : public TNonCopyable
{
public:
    THorizontalSchemalessBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        const std::vector<int>& idMapping,
        int keyColumnCount);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKey(const TOwningKey& key);
    
    const TOwningKey& GetKey() const;
    TUnversionedRow GetRow(TChunkedMemoryPool* memoryPool);
    const char* GetRowPointer() const;

    i64 GetRowIndex() const;

    static TUnversionedRow GetRow(const char* rowPointer, TChunkedMemoryPool* memoryPool);

private:
    TSharedRef Block_;
    NProto::TBlockMeta Meta_;

    // Maps chunk name table ids to client name table ids.
    std::vector<int> IdMapping_;
    int KeyColumnCount_;

    bool Closed_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* RowPointer_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    TOwningKey Key_;


    bool JumpToRowIndex(i64 rowIndex);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
