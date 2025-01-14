#pragma once

#include "evaluation_helpers.h"

#include <yt/yt/library/query/base/vector_over_memory_chunk_provider.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TTopCollector
{
public:
    using TComparerFunction = NWebAssembly::TCompartmentFunction<TComparerFunction>;

    TTopCollector(
        i64 limit,
        TComparerFunction comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    std::vector<const TPIValue*> GetRows() const;

    // Returns a pointer to the captured row if |row| was added.
    template <class TOnInsert, class TOnEvict>
    const TPIValue* AddRow(const TPIValue* row, TOnInsert onInsert, TOnEvict onEvict);
    Y_FORCE_INLINE const TPIValue* AddRow(const TPIValue* row);

private:
    struct TRowAndBuffer
    {
        TPIValue* Row = nullptr;
        i64 ContextIndex = -1;
    };

    void AccountGarbage(const TPIValue* row);
    void CollectGarbageAndAllocateNewContextIfNeeded();
    TRowAndBuffer Capture(const TPIValue* row, TPIValue* destination);

    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    const TComparerFunction Comparer_;
    const size_t RowSize_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const i64 Limit_;

    TExpressionContext RowsContext_;

    std::vector<TExpressionContext> StringLikeValueContexts_;
    std::vector<int> StringLikeValueEmptyContextIds_;

    TVectorOverMemoryChunkProvider<TRowAndBuffer> Heap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define TOP_COLLECTOR_INL_H
#include "top_collector-inl.h"
#undef TOP_COLLECTOR_INL_H
