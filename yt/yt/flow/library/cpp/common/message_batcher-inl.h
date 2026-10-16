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
    TOutputCallback&& outputCallback)
{
    using TInputQueue = std::pair<TSet*, std::function<TPriority()>>;

    EraseIf(inputQueues, [] (const auto& queue) {
        return queue.first->empty();
    });

    if (inputQueues.empty()) {
        return;
    }

    auto queuesComparator = [] (const TInputQueue& lhs, const TInputQueue& rhs) {
        return lhs.second() > rhs.second();
    };

    std::priority_queue<TInputQueue, std::vector<TInputQueue>, decltype(queuesComparator)> heap(queuesComparator, std::move(inputQueues));

    while (!heap.empty()) {
        auto [queue, priorityGetter] = heap.top();
        heap.pop();

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
            heap.push({queue, priorityGetter});
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
