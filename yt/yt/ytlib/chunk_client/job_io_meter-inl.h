#ifndef JOB_IO_METER_INL_H_
#error "Direct inclusion of this file is not allowed, include job_io_meter.h"
// For the sake of sane code completion.
#include "job_io_meter.h"
#endif

#include "chunk_reader_options.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoConsumed(
    const TRequestPtr& req,
    const TClientChunkOptions& options)
{
    if (const auto& jobIoMeter = options.JobIoMeter; jobIoMeter && jobIoMeter->IsEnabled()) {
        req->set_io_consumed(jobIoMeter->GetIoConsumedInWindow());
        if (auto consumerId = jobIoMeter->GetConsumerId(); !consumerId.IsEmpty()) {
            ToProto(req->mutable_io_consumer_id(), consumerId);
        }
    }
}

template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoFairShareWeight(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    std::optional<double> configuredWeight)
{
    if (auto weight = GetEffectiveIoFairShareWeight(configuredWeight, options.JobIoMeter)) {
        req->set_io_fair_share_weight(*weight);
    }
}

template <class TRequestPtr>
void SetRequestReadIOFairShareState(
    const TRequestPtr& req,
    const TClientChunkReadOptions& options,
    std::optional<double> configuredWeight)
{
    if (const auto& state = options.ForwardedIOFairShareState) {
        if (state->IOConsumed) {
            req->set_io_consumed(*state->IOConsumed);
        }
        if (state->IOFairShareWeight) {
            req->set_io_fair_share_weight(*state->IOFairShareWeight);
        }
        if (!state->IOConsumerId.IsEmpty()) {
            ToProto(req->mutable_io_consumer_id(), state->IOConsumerId);
        }
        return;
    }

    SetRequestIoConsumed(req, options);
    SetRequestIoFairShareWeight(req, options, configuredWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
