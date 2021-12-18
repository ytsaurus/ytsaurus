#pragma once

#include "public.h"

#include <yt/yt/core/misc/optional.h>

namespace NYT::NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! A device for accumulating information on traffic generation (and timing it).
/*!
 * The traffic is measured along 'directions', which are just edges in a
 * directed DC graph (the graph is assumed complete). This edges may originate
 * from or terminate on any DC, not just the local DC. This is because one might
 * generate traffic indirectly by requesting one remote side to send data to
 * another remote side. The DC graph also includes the 'null' data center - for
 * those cases when a node has no associated rack or its rack has no associated
 * data center.
 *
 * In addition to tracking traffic for each DC pair, we also track inbound and
 * outbound traffic explicitly. Inbound traffic originates on local machine and
 * thus also contributes to the 'local DC - some remote DC' directions.
 * Symmetrically, outbound traffic contributes to the 'some remote DC - local DC'
 * directions.
 */
class TTrafficMeter
    : public TRefCounted
{
public:
    using TDirection = std::pair<TDataCenterName, TDataCenterName>;

    explicit TTrafficMeter(const TDataCenterName& localDataCenter);

    //! Marks the start time of data transfer (in any direction).
    void Start(TInstant now = TInstant::Now());

    //! May be called at any time regardless of #Start().
    void IncrementInboundByteCount(const TDataCenterName& sourceDataCenter, i64 byteCount);
    void IncrementOutboundByteCount(const TDataCenterName& destinationDataCenter, i64 byteCount);
    void IncrementByteCount(
        const TDataCenterName& sourceDataCenter,
        const TDataCenterName& destinationDataCenter,
        i64 byteCount);

    THashMap<TDataCenterName, i64> GetInboundByteCountBySource() const;
    THashMap<TDataCenterName, i64> GetOutboundByteCountByDestination() const;
    THashMap<TDirection, i64> GetByteCountByDirection() const;

    TDuration GetDuration() const;

private:
    const TDataCenterName LocalDataCenter_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TInstant StartTime_ = TInstant::Zero();
    THashMap<TDirection, i64> Data_;
    THashMap<TDataCenterName, i64> InboundData_;
    THashMap<TDataCenterName, i64> OutboundData_;

    template <typename T>
    void IncrementByteCountImpl(THashMap<T, i64>& data, const T& key, i64 byteCount);
};

DEFINE_REFCOUNTED_TYPE(TTrafficMeter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define TRAFFIC_METER_INL_H_
#include "traffic_meter-inl.h"
#undef TRAFFIC_METER_INL_H_
