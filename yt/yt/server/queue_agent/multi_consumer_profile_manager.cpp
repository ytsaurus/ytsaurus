#include "multi_consumer_profile_manager.h"

#include "snapshot.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueueAgent {

using namespace NProfiling;
using namespace NLogging;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMultiConsumerCounters
{
    TGauge Consumers;

    TMultiConsumerCounters(const TProfiler& profiler)
        : Consumers(profiler.Gauge("/consumers"))
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiConsumerProfileManager
    : public IMultiConsumerProfileManager
{
public:
    TMultiConsumerProfileManager(
        const TProfiler& profiler,
        const TLogger& logger,
        const TConsumerTableRow& row)
        : IMultiConsumerProfileManager({
            {EProfilerScope::Object,
                profiler
                    .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::MultiConsumer>(row))
                    .WithGlobal()
                    .WithPrefix("/multi_consumer")},
            {EProfilerScope::ObjectPass,
                profiler
                    .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::MultiConsumer>(row, /*enablePathAggregation*/ true, /*addObjectType*/ true))
                    .WithPrefix("/multi_consumer/controller")},
            {EProfilerScope::ObjectPartition, profiler},
            {
                EProfilerScope::AlertManager,
                profiler
                    .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::MultiConsumer>(row, /*enablePathAggregation*/ true))
                    .WithGlobal()
                    .WithPrefix("/multi_consumer/controller")
            }
        })
        , MultiConsumerCounters_(GetProfiler(EProfilerScope::Object))
        , Logger(logger)
    { }

    void Profile(
        const TMultiConsumerSnapshotPtr& /*previousSnapshot*/,
        const TMultiConsumerSnapshotPtr& currentSnapshot) override
    {
        YT_LOG_DEBUG("Updating counters");
        MultiConsumerCounters_.Consumers.Update(currentSnapshot->QueueConsumerNames.size());
    }

private:
    const TMultiConsumerCounters MultiConsumerCounters_;
    const TLogger Logger;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IMultiConsumerProfileManagerPtr CreateMultiConsumerProfileManager(
    const TProfiler& profiler,
    const TLogger& logger,
    const TConsumerTableRow& row)
{
    return New<TMultiConsumerProfileManager>(profiler, logger, row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
