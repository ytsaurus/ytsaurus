#include "queue_static_table_export_manager.h"

#include "config.h"

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NQueueAgent {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TQueueStaticTableExportManager
    : public IQueueStaticTableExportManager
{
public:
    TQueueStaticTableExportManager(
        const TQueueStaticTableExportManagerDynamicConfigPtr& dynamicConfig);

    IThroughputThrottlerPtr GetExportThrottler() const override;

    void OnDynamicConfigChanged(
        const TQueueStaticTableExportManagerDynamicConfigPtr& oldConfig,
        const TQueueStaticTableExportManagerDynamicConfigPtr& newConfig) override;

private:
    TLogger Logger;

    TProfiler Profiler_;
    IReconfigurableThroughputThrottlerPtr Throttler_;

    TQueueStaticTableExportManagerDynamicConfigPtr DynamicConfig_;
};

////////////////////////////////////////////////////////////////////////////////

TQueueStaticTableExportManager::TQueueStaticTableExportManager(
    const TQueueStaticTableExportManagerDynamicConfigPtr& dynamicConfig)
    : Logger(QueueStaticTableExportManagerLogger())
    , Profiler_(QueueAgentProfiler()
        .WithPrefix("static_export_manager"))
    , Throttler_(CreateNamedReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(dynamicConfig->ExportRateLimit),
        /*name*/ "ExportThrottler",
        Logger,
        Profiler_)
    )
    , DynamicConfig_(dynamicConfig)
{ }

IThroughputThrottlerPtr TQueueStaticTableExportManager::GetExportThrottler() const
{
    return Throttler_;
}

void TQueueStaticTableExportManager::OnDynamicConfigChanged(
    const TQueueStaticTableExportManagerDynamicConfigPtr& oldConfig,
    const TQueueStaticTableExportManagerDynamicConfigPtr& newConfig)
{
    Throttler_->Reconfigure(TThroughputThrottlerConfig::Create(newConfig->ExportRateLimit));

    YT_LOG_DEBUG("Updated queue static table export manager dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

IQueueStaticTableExportManagerPtr CreateQueueStaticTableExportManager(
    const TQueueStaticTableExportManagerDynamicConfigPtr& dynamicConfig)
{
    return New<TQueueStaticTableExportManager>(
        dynamicConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
