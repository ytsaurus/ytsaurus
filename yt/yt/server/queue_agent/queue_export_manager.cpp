#include "queue_export_manager.h"

#include "config.h"

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NQueueAgent {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TQueueExportManager
    : public IQueueExportManager
{
public:
    explicit TQueueExportManager(
        TQueueExportManagerDynamicConfigPtr dynamicConfig)
        : Logger(QueueExportManagerLogger())
        , Profiler_(QueueAgentProfiler()
            .WithPrefix("static_export_manager"))
        , Throttler_(CreateNamedReconfigurableThroughputThrottler(
            TThroughputThrottlerConfig::Create(dynamicConfig->ExportRateLimit),
            /*name*/ "ExportThrottler",
            Logger,
            Profiler_))
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    IThroughputThrottlerPtr GetExportThrottler() const override
    {
        return Throttler_;
    }

    void OnDynamicConfigChanged(
        const TQueueExportManagerDynamicConfigPtr& oldConfig,
        const TQueueExportManagerDynamicConfigPtr& newConfig) override
    {
        Throttler_->Reconfigure(TThroughputThrottlerConfig::Create(newConfig->ExportRateLimit));
        DynamicConfig_ = newConfig;

        YT_LOG_DEBUG("Updated queue export manager dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    const TLogger Logger;

    const TProfiler Profiler_;
    const IReconfigurableThroughputThrottlerPtr Throttler_;

    TQueueExportManagerDynamicConfigPtr DynamicConfig_;
};

////////////////////////////////////////////////////////////////////////////////

IQueueExportManagerPtr CreateQueueExportManager(
    TQueueExportManagerDynamicConfigPtr dynamicConfig)
{
    return New<TQueueExportManager>(
        std::move(dynamicConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
