#pragma once
#ifndef SLIDING_WINDOW_INL_H_
#error "Direct inclusion of this file is not allowed, include sliding_window.h"
// For the sake of sane code completion.
#include "sliding_window.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket>
TSlidingWindow<TPacket>::TSlidingWindow(ssize_t maxSize)
    : MaxSize_(maxSize)
{ }

template <typename TPacket>
template <class F>
void TSlidingWindow<TPacket>::AddPacket(
    ssize_t sequenceNumber,
    TPacket&& packet,
    const F& callback)
{
    if (sequenceNumber < GetNextSequenceNumber()) {
        THROW_ERROR_EXCEPTION("Packet sequence number is too small")
            << TErrorAttribute("sequence_number", sequenceNumber)
            << TErrorAttribute("min_sequence_number", GetNextSequenceNumber());
    }

    if (Window_.find(sequenceNumber) != Window_.end()) {
        THROW_ERROR_EXCEPTION("Packet with this sequence number is already queued")
            << TErrorAttribute("sequence_number", sequenceNumber);
    }

    if (Window_.size() >= MaxSize_) {
        THROW_ERROR_EXCEPTION("Packet window overflow")
            << TErrorAttribute("max_size", MaxSize_);
    }

    Window_[sequenceNumber] = std::move(packet);

    for (auto it = Window_.find(NextPacketSequenceNumber_);
        it != Window_.end();
        it = Window_.find(++NextPacketSequenceNumber_))
    {
        callback(std::move(it->second));
        Window_.erase(it);
    }
}

template <typename TPacket>
ssize_t TSlidingWindow<TPacket>::GetNextSequenceNumber() const
{
    return NextPacketSequenceNumber_;
}

template <typename TPacket>
bool TSlidingWindow<TPacket>::IsEmpty() const
{
    return Window_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
