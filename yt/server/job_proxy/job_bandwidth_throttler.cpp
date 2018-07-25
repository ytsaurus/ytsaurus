#include "job_bandwidth_throttler.h"

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NExecAgent;

////////////////////////////////////////////////////////////////////////////////

class TJobBandwidthThrottler
    : public IThroughputThrottler
{
public:
    TJobBandwidthThrottler(
        const IChannelPtr& channel,
        EJobBandwidthDirection direction,
        const TWorkloadDescriptor& descriptor,
        TJobId jobId,
        TDuration timeout)
        : Proxy_(channel)
        , Direction_(direction)
        , Descriptor_(descriptor)
        , JobId_(jobId)
    {
        Proxy_.SetDefaultTimeout(timeout);
    }

    virtual TFuture<void> Throttle(i64 count) override
    {
        auto request = Proxy_.ThrottleBandwidth();
        request->set_direction(static_cast<i32>(Direction_));
        request->set_byte_count(count);
        ToProto(request->mutable_workload_descriptor(), Descriptor_);
        ToProto(request->mutable_job_id(), JobId_);
        return request->Invoke()
            .As<void>();
    }

    virtual bool TryAcquire(i64 count) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual void Acquire(i64 count) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual bool IsOverdraft() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual i64 GetQueueTotalCount() const override
    {
        Y_UNIMPLEMENTED();
    }

private:
    TSupervisorServiceProxy Proxy_;

    EJobBandwidthDirection Direction_;

    TWorkloadDescriptor Descriptor_;

    TJobId JobId_;
};

////////////////////////////////////////////////////////////////////////////////

IThroughputThrottlerPtr CreateInJobBandwidthThrottler(
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId,
    TDuration timeout)
{
    return New<TJobBandwidthThrottler>(
        channel,
        EJobBandwidthDirection::In,
        descriptor,
        jobId,
        timeout);
}

IThroughputThrottlerPtr CreateOutJobBandwidthThrottler(
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId,
    TDuration timeout)
{
    return New<TJobBandwidthThrottler>(
        channel,
        EJobBandwidthDirection::Out,
        descriptor,
        jobId,
        timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
