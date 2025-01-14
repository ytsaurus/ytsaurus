#include "top_collector.h"

#include "position_independent_value_transfer.h"

#include <yt/yt/core/misc/heap.h>

namespace NYT::NQueryClient {

using namespace NWebAssembly;

////////////////////////////////////////////////////////////////////////////////

constexpr ssize_t BufferLimit = 512_KB;

struct TTopCollectorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(
    i64 limit,
    TComparerFunction comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider)
    : Comparer_(comparer)
    , RowSize_(rowSize)
    , MemoryChunkProvider_(std::move(memoryChunkProvider))
    , Limit_(limit)
    , RowsContext_(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_))
    , Heap_(GetRefCountedTypeCookie<TTopCollectorBufferTag>(), MemoryChunkProvider_)
{ }

std::vector<const TPIValue*> TTopCollector::GetRows() const
{
    auto result = std::vector<const TPIValue*>(Heap_.Size());

    for (i64 index = 0; index < Heap_.Size(); ++index) {
        result[index] = Heap_[index].Row;
    }

    std::sort(result.begin(), result.end(), Comparer_);

    return result;
}

void TTopCollector::AccountGarbage(const TPIValue* row)
{
    row = ConvertPointerFromWasmToHost(row, RowSize_);
    for (size_t index = 0; index < RowSize_; ++index) {
        auto& value = row[index];
        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::CollectGarbageAndAllocateNewContextIfNeeded()
{
    if (!StringLikeValueEmptyContextIds_.empty()) {
        return;
    }

    if (GarbageMemorySize_ <= TotalMemorySize_ / 2) {
        // Allocate new context and add to emptyContextIds.
        StringLikeValueEmptyContextIds_.push_back(StringLikeValueContexts_.size());
        StringLikeValueContexts_.push_back(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_));
        return;
    }

    // Collect garbage.
    auto contextsToRows = std::vector<std::vector<size_t>>(StringLikeValueContexts_.size());
    for (i64 rowId = 0; rowId < Heap_.Size(); ++rowId) {
        contextsToRows[Heap_[rowId].ContextIndex].push_back(rowId);
    }

    auto context = MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_);

    TotalMemorySize_ = 0;
    AllocatedMemorySize_ = 0;
    GarbageMemorySize_ = 0;

    for (size_t contextId = 0; contextId < contextsToRows.size(); ++contextId) {
        i64 savedSize = context.GetSize();

        for (auto rowId : contextsToRows[contextId]) {
            auto* row = Heap_[rowId].Row;

            for (size_t index = 0; index < RowSize_; ++index) {
                CapturePIValue(
                    &context,
                    &row[index],
                    EAddressSpace::WebAssembly,
                    EAddressSpace::WebAssembly);
            }
        }

        AllocatedMemorySize_ += context.GetSize() - savedSize;

        TotalMemorySize_ += context.GetCapacity();

        if (context.GetSize() < BufferLimit) {
            StringLikeValueEmptyContextIds_.push_back(contextId);
        }

        std::swap(context, StringLikeValueContexts_[contextId]);
        context.Clear();
    }

    if (StringLikeValueEmptyContextIds_.empty()) {
        StringLikeValueEmptyContextIds_.push_back(StringLikeValueContexts_.size());
        StringLikeValueContexts_.push_back(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_));
    }
}

TTopCollector::TRowAndBuffer TTopCollector::Capture(
    const TPIValue* row,
    TPIValue* destination)
{
    CollectGarbageAndAllocateNewContextIfNeeded();

    YT_VERIFY(!StringLikeValueEmptyContextIds_.empty());

    auto contextId = StringLikeValueEmptyContextIds_.back();
    auto& context = StringLikeValueContexts_[contextId];

    i64 savedSize = context.GetSize();
    i64 savedCapacity = context.GetCapacity();

    for (size_t index = 0; index < RowSize_; ++index) {
        CopyPositionIndependent(
            ConvertPointerFromWasmToHost(&destination[index]),
            *ConvertPointerFromWasmToHost(&row[index]));

        CapturePIValue(
            &context,
            &destination[index],
            EAddressSpace::WebAssembly,
            EAddressSpace::WebAssembly);
    }

    AllocatedMemorySize_ += context.GetSize() - savedSize;
    TotalMemorySize_ += context.GetCapacity() - savedCapacity;

    if (context.GetSize() >= BufferLimit) {
        StringLikeValueEmptyContextIds_.pop_back();
    }

    return {destination, contextId};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
