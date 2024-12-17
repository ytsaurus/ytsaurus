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
    , Limit_(limit)
    , RowsContext_(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_))
    , Heap_(GetRefCountedTypeCookie<TTopCollectorBufferTag>(), MemoryChunkProvider_)
{ }

const TPIValue* TTopCollectorBase::AddRow(const TPIValue* row)
{
    if (Heap_.Size() < Limit_) {
        auto* destination = std::bit_cast<TPIValue*>(
            RowsContext_.AllocateAligned(
                sizeof(TPIValue) * RowSize_,
                EAddressSpace::WebAssembly));

        auto capturedRow = Capture(row, destination);
        Heap_.PushBack(capturedRow);
        OnInsert(capturedRow.Row);

        if (Heap_.Size() == Limit_) {
            MakeHeap(Heap_.Begin(), Heap_.End(), MaxComparer_);
        }

        return capturedRow.Row;
    }

    if (!Heap_.Empty() && MaxComparer_(Heap_[0].Row, row)) {
        auto popped = Heap_[0].Row;
        AccountGarbage(popped);
        OnEvict(popped);
        auto capturedRow = Capture(row, popped);
        Heap_[0].ContextIndex = capturedRow.ContextIndex;
        AdjustHeapFront(Heap_.Begin(), Heap_.End(), MaxComparer_);
        OnInsert(capturedRow.Row);
        return capturedRow.Row;
    }

    return nullptr;
}

std::vector<const TPIValue*> TTopCollectorBase::GetRows() const
{
    auto result = std::vector<const TPIValue*>(Heap_.Size());

    for (i64 index = 0; index < Heap_.Size(); ++index) {
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

TTopCollectorBase::TRowAndBuffer TTopCollectorBase::Capture(
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

TTopCollectorWithHashMap::TTopCollectorWithHashMap(
    i64 limit,
    TComparerFunction comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider,
    TLookupRows* const hashMap)
    : TTopCollectorBase(limit, comparer, rowSize, std::move(memoryChunkProvider))
    , HashMap_(hashMap)
{ }

void TTopCollectorWithHashMap::OnInsert(const TPIValue* insertedRow)
{
    HashMap_->insert(insertedRow);
}

void TTopCollectorWithHashMap::OnEvict(const TPIValue* evictedRow)
{
    HashMap_->erase(evictedRow);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
