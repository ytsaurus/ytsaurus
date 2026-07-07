#pragma once

#include "evaluation_helpers.h"

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
    using THeap = std::vector<TPIValue*, TAllocatorOverChunkProvider<TPIValue*>>;

    void AccountGarbage(const TPIValue* row);
    void CollectGarbageIfNeeded();
    void Capture(const TPIValue* row, TPIValue* destination);

    // GarbageMemorySize <= StringLikeValuesContext_.GetSize() <= StringLikeValuesContext_.GetCapacity()
    i64 GarbageMemorySize_ = 0;

    const TComparerFunction Comparer_;
    const size_t RowSize_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const i64 Limit_;

    TExpressionContext RowsContext_;
    TExpressionContext StringLikeValuesContext_;

    THeap Heap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define TOP_COLLECTOR_INL_H_
#include "top_collector-inl.h"
#undef TOP_COLLECTOR_INL_H_
