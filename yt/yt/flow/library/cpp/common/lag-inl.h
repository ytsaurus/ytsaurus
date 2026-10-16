#pragma once

#ifndef LAG_INL_H_
    #error "Direct inclusion of this file is not allowed, include lag.h"
    // For the sake of sane code completion.
    #include "lag.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline TDuration ComputeLag(TInstant now, TSystemTimestamp timestamp)
{
    return now.Seconds() > timestamp.Underlying()
        ? TDuration::Seconds(now.Seconds() - timestamp.Underlying())
        : TDuration::Zero();
}

inline TDuration ComputeLag(TSystemTimestamp now, TSystemTimestamp timestamp)
{
    return now > timestamp
        ? TDuration::Seconds(now.Underlying() - timestamp.Underlying())
        : TDuration::Zero();
}

////////////////////////////////////////////////////////////////////////////////

template <class TRange>
void TStreamEventLagObserver::ObserveBatch(const TRange& messages)
{
    auto guard = Guard(Lock_);
    for (const auto& message : messages) {
        DoObserve(message->StreamId, message->EventTimestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
