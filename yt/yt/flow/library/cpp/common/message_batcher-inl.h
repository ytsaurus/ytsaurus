#pragma once

#ifndef MESSAGE_BATCHER_INL_H_
    #error "Direct inclusion of this file is not allowed, include message_batcher.h"
    // For the sake of sane code completion.
    #include "message.h"
#endif

namespace NYT::NFlow {

//////////////////////////////////////////////////////////////////////////////

template <class TSet, class TPriority, class TMessageAccessor, class TOutputCallback>
void MergingExtractBatch(
    std::vector<std::pair<TSet*, std::function<TPriority()>>> inputQueues,
    TMessageAccessor&& messageAccessor,
    TMessageBatchLimiter& batchLimiter,
    TOutputCallback&& outputCallback,
    const std::function<bool(const std::type_identity_t<TPriority>&)>& canExtract)
{
    EraseIf(inputQueues, [] (const auto& queue) {
        return queue.first->empty();
    });

    if (inputQueues.empty()) {
        return;
    }

    // A priority depends only on its queue front, so it is cached until that front is extracted.
    // Optional because a priority may hold references and thus be non-assignable.
    std::vector<std::optional<TPriority>> priorities;
    std::vector<size_t> heap;
    priorities.reserve(inputQueues.size());
    heap.reserve(inputQueues.size());
    for (size_t index = 0; index < inputQueues.size(); ++index) {
        priorities.emplace_back(inputQueues[index].second());
        heap.push_back(index);
    }

    auto queuesComparator = [&] (size_t lhs, size_t rhs) {
        return *priorities[lhs] > *priorities[rhs];
    };
    std::make_heap(heap.begin(), heap.end(), queuesComparator);

    while (!heap.empty()) {
        const size_t index = heap.front();
        if (canExtract && !canExtract(*priorities[index])) {
            break;
        }
        std::pop_heap(heap.begin(), heap.end(), queuesComparator);
        heap.pop_back();
        auto* queue = inputQueues[index].first;

        if constexpr (requires { queue->extract_front(); }) {
            auto value = queue->extract_front();
            batchLimiter.Add(messageAccessor(value));
            outputCallback(value);
        } else {
            auto internalNode = queue->extract(queue->begin());
            batchLimiter.Add(messageAccessor(internalNode.value()));
            outputCallback(internalNode.value());
        }

        if (batchLimiter.IsFull()) {
            break;
        }

        if (!queue->empty()) {
            priorities[index].emplace(inputQueues[index].second());
            heap.push_back(index);
            std::push_heap(heap.begin(), heap.end(), queuesComparator);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
