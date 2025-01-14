#ifndef TOP_COLLECTOR_INL_H
#error "Direct inclusion of this file is not allowed, top_collector.h"
// For the sake of sane code completion.
#include "top_collector.h"
#endif

#include <yt/yt/core/misc/heap.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TOnInsert, class TOnEvict>
const TPIValue* TTopCollector::AddRow(const TPIValue* row, TOnInsert onInsert, TOnEvict onEvict)
{
    if (Heap_.Size() < Limit_) {
        auto* destination = std::bit_cast<TPIValue*>(
            RowsContext_.AllocateAligned(
                sizeof(TPIValue) * RowSize_,
                NWebAssembly::EAddressSpace::WebAssembly));

        auto capturedRow = Capture(row, destination);
        Heap_.PushBack(capturedRow);
        onInsert(capturedRow.Row);

        if (Heap_.Size() == Limit_) {
            MakeHeap(Heap_.Begin(), Heap_.End(), [&] (const auto& lhs, const auto& rhs) {
                // NB: Inverse |lhs| and |rhs|.
                return Comparer_(rhs.Row, lhs.Row);
            });
        }

        return capturedRow.Row;
    }

    if (!Heap_.Empty() && Comparer_(row, Heap_[0].Row)) {
        auto popped = Heap_[0].Row;
        AccountGarbage(popped);
        onEvict(popped);
        auto capturedRow = Capture(row, popped);
        Heap_[0].ContextIndex = capturedRow.ContextIndex;
        AdjustHeapFront(Heap_.Begin(), Heap_.End(), [&] (const auto& lhs, const auto& rhs) {
            // NB: Inverse |lhs| and |rhs|.
            return Comparer_(rhs.Row, lhs.Row);
        });
        onInsert(capturedRow.Row);
        return capturedRow.Row;
    }

    return nullptr;
}

const TPIValue* TTopCollector::AddRow(const TPIValue* row)
{
    return AddRow(row, [] (const TPIValue*) {}, [] (const TPIValue*) {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
