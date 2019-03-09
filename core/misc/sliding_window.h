#pragma once

#include "error.h"
#include "public.h"

#include <yt/core/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A classic sliding window implementation.
/*!
 *  Can defer up to #windowSize "packets" (abstract movable objects) and reorder
 *  them according to their sequence numbers. If #windowSize is not set, it's
 *  assumed to be infinity.
 *
 *  Once a packet is received from the outside world, the user should call
 *  #AddPacket, providing packet's sequence number.
 *
 *  The #callback is called once for each packet when it's about to be popped
 *  out of the window. Specifically, a packet leaves the window when no
 *  packets preceding it are missing.
 *
 *  #callback mustn't throw.
 */

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket, typename TDerived>
class TSlidingWindowBase
{
public:
    using TOnPacketCallback = TCallback<void(TPacket&&)>;

    //! Constructs the sliding window.
    TSlidingWindowBase(const TOnPacketCallback& callback);

    //! Informs the window that the packet has been received.
    /*!
    *  May cause the #callback to be called for deferred packets (up to
    *  #WindowSize times).
    *
    *  Throws if a packet with the specified sequence number has already been
    *  set.
    *  Throws if the sequence number was already slid over (i.e. it's too
    *  small).
    *  Throws if setting this packet would exceed the window size (i.e. the
    *  sequence number is too large).
    */
    void AddPacket(size_t sequenceNumber, TPacket&& packet);

    //! Checks whether the window stores no packets.
    bool Empty() const;

    //! Returns the first missing sequence number.
    inline size_t GetNextSequenceNumber() const;

protected:
    TOnPacketCallback Callback_;
    size_t NextPacketSequenceNumber_ = 0;

private:
    inline TDerived* Derived();
    inline const TDerived* Derived() const;
    inline bool Contains(size_t sequenceNumber) const;
    void MaybeSlideWindow();
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPacket, size_t WindowSize = 0>
class TSlidingWindow
    : public TSlidingWindowBase<TPacket, TSlidingWindow<TPacket, WindowSize>>
{
private:
    using TBase = TSlidingWindowBase<TPacket, TSlidingWindow<TPacket, WindowSize>>;

public:
    TSlidingWindow(const typename TBase::TOnPacketCallback& callback);

private:
    using TPosition = const std::optional<TPacket>*;
    using TMutablePosition = std::optional<TPacket>*;
    friend TBase;

    std::array<std::optional<TPacket>, WindowSize> Window_;

    size_t NextPacketIndex_ = 0;
    size_t DeferredPacketCount_ = 0;

    size_t GetMaxSequenceNumber() const;
    bool IsEmpty() const;
    TMutablePosition Find(size_t sequenceNumber);
    TPosition Find(size_t sequenceNumber) const;
    bool HasPacket(TPosition position) const;
    void DoSetPacket(size_t sequenceNumber, TPacket&& packet);
    void SingleSlide(TMutablePosition position);

    size_t GetPacketSlot(size_t sequenceNumber) const;
};

template <typename TPacket>
class TSlidingWindow<TPacket, 0>
    : public TSlidingWindowBase<TPacket, TSlidingWindow<TPacket, 0>>
{
private:
    using TBase = TSlidingWindowBase<TPacket, TSlidingWindow<TPacket, 0>>;

public:
    TSlidingWindow(const typename TBase::TOnPacketCallback& callback);

private:
    using TPosition = typename THashMap<size_t, TPacket>::const_iterator;
    using TMutablePosition = typename THashMap<size_t, TPacket>::iterator;
    friend TBase;

    THashMap<size_t, TPacket> Window_;

    size_t GetMaxSequenceNumber() const;
    bool IsEmpty() const;
    TMutablePosition Find(size_t sequenceNumber);
    TPosition Find(size_t sequenceNumber) const;
    bool HasPacket(TPosition position) const;
    void DoSetPacket(size_t sequenceNumber, TPacket&& packet);
    void SingleSlide(TMutablePosition position);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SLIDING_WINDOW_INL_H_
#include "sliding_window-inl.h"
#undef SLIDING_WINDOW_INL_H_
