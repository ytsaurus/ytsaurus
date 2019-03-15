#pragma once
#ifndef SLIDING_WINDOW_INL_H_
#error "Direct inclusion of this file is not allowed, include sliding_window.h"
// For the sake of sane code completion.
#include "sliding_window.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket, typename TDerived>
TSlidingWindowBase<TPacket, TDerived>::TSlidingWindowBase(
    const TOnPacketCallback& callback)
    : Callback_(callback)
{ }

template <typename TPacket, typename TDerived>
void TSlidingWindowBase<TPacket, TDerived>::AddPacket(
    size_t sequenceNumber,
    TPacket&& packet)
{
    if (sequenceNumber < GetNextSequenceNumber()) {
        THROW_ERROR_EXCEPTION("Packet sequence number is too small")
            << TErrorAttribute("sequence_number", sequenceNumber)
            << TErrorAttribute("min_sequence_number", GetNextSequenceNumber());
    }

    if (sequenceNumber > Derived()->GetMaxSequenceNumber()) {
        THROW_ERROR_EXCEPTION("Packet sequence number is too large")
            << TErrorAttribute("sequence_number", sequenceNumber)
            << TErrorAttribute("max_sequence_number", Derived()->GetMaxSequenceNumber());
    }

    if (Derived()->Contains(sequenceNumber)) {
        THROW_ERROR_EXCEPTION("Packet with this sequence number already exists")
            << TErrorAttribute("sequence_number", sequenceNumber);
    }

    Derived()->DoSetPacket(sequenceNumber, std::move(packet));
    Derived()->MaybeSlideWindow();
}

template <typename TPacket, typename TDerived>
bool TSlidingWindowBase<TPacket, TDerived>::Empty() const
{
    return Derived()->IsEmpty();
}

template <typename TPacket, typename TDerived>
size_t TSlidingWindowBase<TPacket, TDerived>::GetNextSequenceNumber() const
{
    return NextPacketSequenceNumber_;
}

template <typename TPacket, typename TDerived>
TDerived* TSlidingWindowBase<TPacket, TDerived>::Derived()
{
    return static_cast<TDerived*>(this);
}

template <typename TPacket, typename TDerived>
const TDerived* TSlidingWindowBase<TPacket, TDerived>::Derived() const
{
    return static_cast<const TDerived*>(this);
}

template <typename TPacket, typename TDerived>
bool TSlidingWindowBase<TPacket, TDerived>::Contains(size_t sequenceNumber) const
{
    return Derived()->HasPacket(Derived()->Find(sequenceNumber));
}

template <typename TPacket, typename TDerived>
void TSlidingWindowBase<TPacket, TDerived>::MaybeSlideWindow()
{
    for (auto it = Derived()->Find(NextPacketSequenceNumber_);
         Derived()->HasPacket(it);
         it = Derived()->Find(++NextPacketSequenceNumber_))
    {
        Derived()->SingleSlide(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket, size_t WindowSize>
TSlidingWindow<TPacket, WindowSize>::TSlidingWindow(
    const typename TBase::TOnPacketCallback& callback)
    : TBase(callback)
{ }

template <typename TPacket, size_t WindowSize>
std::optional<TPacket>* TSlidingWindow<TPacket, WindowSize>::Find(
    size_t sequenceNumber)
{
    return Window_.begin() + GetPacketSlot(sequenceNumber);
}

template <typename TPacket, size_t WindowSize>
const std::optional<TPacket>* TSlidingWindow<TPacket, WindowSize>::Find(
    size_t sequenceNumber) const
{
    return Window_.begin() + GetPacketSlot(sequenceNumber);
}

template <typename TPacket, size_t WindowSize>
bool TSlidingWindow<TPacket, WindowSize>::HasPacket(
    const std::optional<TPacket>* position) const
{
    return position->has_value();
}

template <typename TPacket, size_t WindowSize>
size_t TSlidingWindow<TPacket, WindowSize>::GetMaxSequenceNumber() const
{
    return this->NextPacketSequenceNumber_ + WindowSize - 1;
}

template <typename TPacket, size_t WindowSize>
void TSlidingWindow<TPacket, WindowSize>::DoSetPacket(size_t sequenceNumber, TPacket&& packet)
{
    *Find(sequenceNumber) = std::move(packet);
    ++DeferredPacketCount_;
}

template <typename TPacket, size_t WindowSize>
void TSlidingWindow<TPacket, WindowSize>::SingleSlide(std::optional<TPacket>* position)
{
    TBase::Callback_(*std::move(*position));
    position->reset();
    YCHECK(DeferredPacketCount_ > 0);
    --DeferredPacketCount_;
    NextPacketIndex_ = (NextPacketIndex_ + 1) % WindowSize;
}

template <typename TPacket, size_t WindowSize>
bool TSlidingWindow<TPacket, WindowSize>::IsEmpty() const
{
    return DeferredPacketCount_ == 0;
}

template <typename TPacket, size_t WindowSize>
size_t TSlidingWindow<TPacket, WindowSize>::GetPacketSlot(
    size_t sequenceNumber) const
{
    return (sequenceNumber - TBase::NextPacketSequenceNumber_ + NextPacketIndex_) % WindowSize;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket>
TSlidingWindow<TPacket, 0>::TSlidingWindow(
    const typename TBase::TOnPacketCallback& callback)
    : TBase(callback)
{ }

template <typename TPacket>
typename THashMap<size_t, TPacket>::iterator TSlidingWindow<TPacket, 0>::Find(
    size_t sequenceNumber)
{
    return Window_.find(sequenceNumber);
}

template <typename TPacket>
typename THashMap<size_t, TPacket>::const_iterator TSlidingWindow<TPacket, 0>::Find(
    size_t sequenceNumber) const
{
    return Window_.find(sequenceNumber);
}

template <typename TPacket>
bool TSlidingWindow<TPacket, 0>::HasPacket(
    typename THashMap<size_t, TPacket>::const_iterator position) const
{
    return position != Window_.end();
}

template <typename TPacket>
size_t TSlidingWindow<TPacket, 0>::GetMaxSequenceNumber() const
{
    return std::numeric_limits<size_t>::max();
}

template <typename TPacket>
void TSlidingWindow<TPacket, 0>::DoSetPacket(size_t sequenceNumber, TPacket&& packet)
{
    Window_[sequenceNumber] = std::move(packet);
}

template <typename TPacket>
void TSlidingWindow<TPacket, 0>::SingleSlide(
    typename THashMap<size_t, TPacket>::iterator position)
{
    TBase::Callback_(std::move(position->second));
    Window_.erase(position);
}

template <typename TPacket>
bool TSlidingWindow<TPacket, 0>::IsEmpty() const
{
    return Window_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
