#include "top_collector.h"

#include "position_independent_value_transfer.h"

#include <yt/yt/core/misc/heap.h>

namespace NYT::NQueryClient {

using namespace NWebAssembly;

////////////////////////////////////////////////////////////////////////////////

struct TTopCollectorBufferTag
{ };

static constexpr i64 MinimumGarbageCollectionCapacity = 2_MB;

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
    , StringLikeValuesContext_(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_))
    , Heap_(THeap::allocator_type(MemoryChunkProvider_, GetRefCountedTypeCookie<TTopCollectorBufferTag>()))
{ }

std::vector<const TPIValue*> TTopCollector::GetRows() const
{
    auto result = std::vector<const TPIValue*>(Heap_.size());

    for (i64 index = 0; index < std::ssize(Heap_); ++index) {
        result[index] = Heap_[index];
    }

    std::sort(result.begin(), result.end(), Comparer_);

    return result;
}

void TTopCollector::AccountGarbage(const TPIValue* row)
{
    row = PtrFromVM(GetCurrentCompartment(), row, RowSize_);
    for (size_t index = 0; index < RowSize_; ++index) {
        auto& value = row[index];
        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::CollectGarbageIfNeeded()
{
    auto oldCapacity = StringLikeValuesContext_.GetCapacity();
    if (GarbageMemorySize_ <= oldCapacity / 2 ||
        oldCapacity < MinimumGarbageCollectionCapacity)
    {
        return;
    }

    auto context = MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_);

    auto* compartment = GetCurrentCompartment();

    for (auto* row : Heap_) {
        for (size_t index = 0; index < RowSize_; ++index) {
            CapturePIValue(
                compartment,
                &context,
                &row[index]);
        }
    }

    GarbageMemorySize_ = 0;
    StringLikeValuesContext_ = std::move(context);
}

void TTopCollector::Capture(
    const TPIValue* row,
    TPIValue* destination)
{
    auto* compartment = GetCurrentCompartment();

    for (size_t index = 0; index < RowSize_; ++index) {
        CopyPositionIndependent(
            PtrFromVM(compartment, &destination[index]),
            *PtrFromVM(compartment, &row[index]));

        CapturePIValue(
            compartment,
            &StringLikeValuesContext_,
            &destination[index]);
    }

    CollectGarbageIfNeeded();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
