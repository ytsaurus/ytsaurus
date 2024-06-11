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

TTopCollectorBase::TTopCollectorBase(
    i64 limit,
    TComparerFunction comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider)
    : MinComparer_(comparer)
    , MaxComparer_(comparer)
    , RowSize_(rowSize)
    , MemoryChunkProvider_(std::move(memoryChunkProvider))
    , DataContext_(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_))
    , Data_(MakeMutableRange(
        std::bit_cast<TPIValue*>(
            DataContext_.AllocateAligned(sizeof(TPIValue) * rowSize * limit,
            EAddressSpace::WebAssembly)),
        rowSize * limit))
{
    Heap_.reserve(limit);
}

const TPIValue* TTopCollectorBase::AddRow(const TPIValue* row)
{
    if (Heap_.size() < Heap_.capacity()) {
        auto* destination = &Data_[Heap_.size() * RowSize_];
        auto capturedRow = Capture(row, destination);
        Heap_.emplace_back(capturedRow);
        AdjustHeapBack(Heap_.begin(), Heap_.end(), MaxComparer_);
        OnInsert(capturedRow.Row);
        return capturedRow.Row;
    }

    if (!Heap_.empty() && MaxComparer_(Heap_.front().Row, row)) {
        auto popped = Heap_.front().Row;
        AccountGarbage(popped);
        OnEvict(popped);
        auto capturedRow = Capture(row, popped);
        AdjustHeapFront(Heap_.begin(), Heap_.end(), MaxComparer_);
        OnInsert(capturedRow.Row);
        return capturedRow.Row;
    }

    return nullptr;
}

std::vector<const TPIValue*> TTopCollectorBase::GetRows() const
{
    auto result = std::vector<const TPIValue*>(Heap_.size());

    for (size_t index = 0; index < Heap_.size(); ++index) {
        result[index] = Heap_[index].Row;
    }

    std::sort(result.begin(), result.end(), MinComparer_);

    return result;
}

void TTopCollectorBase::AccountGarbage(const TPIValue* row)
{
    row = ConvertPointerFromWasmToHost(row, RowSize_);
    for (size_t index = 0; index < RowSize_; ++index) {
        auto& value = row[index];
        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollectorBase::CollectGarbageAndAllocateNewContextIfNeeded()
{
    if (!EmptyContextIds_.empty()) {
        return;
    }

    if (GarbageMemorySize_ <= TotalMemorySize_ / 2) {
        // Allocate new context and add to emptyContextIds.
        EmptyContextIds_.push_back(Contexts_.size());
        Contexts_.push_back(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_));
        return;
    }

    // Collect garbage.
    auto contextsToRows = std::vector<std::vector<size_t>>(Contexts_.size());
    for (size_t rowId = 0; rowId < Heap_.size(); ++rowId) {
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
            EmptyContextIds_.push_back(contextId);
        }

        std::swap(context, Contexts_[contextId]);
        context.Clear();
    }
}

TTopCollectorBase::TRowAndBuffer TTopCollectorBase::Capture(
    const TPIValue* row,
    TPIValue* destination)
{
    CollectGarbageAndAllocateNewContextIfNeeded();

    YT_VERIFY(!EmptyContextIds_.empty());

    auto contextId = EmptyContextIds_.back();
    auto& context = Contexts_[contextId];

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
        EmptyContextIds_.pop_back();
    }

    return {destination, contextId};
}

////////////////////////////////////////////////////////////////////////////////

TTopCollectorBase::TMinComparer::TMinComparer(TComparerFunction comparer)
    : Comparer_(comparer)
{ }

bool TTopCollectorBase::TMinComparer::operator()(const TRowAndBuffer& lhs, const TRowAndBuffer& rhs) const
{
    return (*this)(lhs.Row, rhs.Row);
}

bool TTopCollectorBase::TMinComparer::operator()(const TPIValue* lhs, const TPIValue* rhs) const
{
    return Comparer_(lhs, rhs);
}

TTopCollectorBase::TMaxComparer::TMaxComparer(TComparerFunction comparer)
    : Comparer_(comparer)
{ }

bool TTopCollectorBase::TMaxComparer::operator()(const TRowAndBuffer& lhs, const TRowAndBuffer& rhs) const
{
    return (*this)(lhs.Row, rhs.Row);
}

bool TTopCollectorBase::TMaxComparer::operator()(const TPIValue* lhs, const TPIValue* rhs) const
{
    return Comparer_(rhs, lhs); // NB: Inverse |lhs| and |rhs|.
}

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(
    i64 limit,
    TComparerFunction comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider)
    : TTopCollectorBase(limit, comparer, rowSize, std::move(memoryChunkProvider))
{ }

void TTopCollector::OnInsert(const TPIValue* /*insertedRow*/)
{
    // Do nothing.
}

void TTopCollector::OnEvict(const TPIValue* /*evictedRow*/)
{
    // Do nothing.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
