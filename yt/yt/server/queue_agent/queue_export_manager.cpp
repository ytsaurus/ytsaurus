#include "queue_export_manager.h"

#include "config.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NQueueAgent {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NProfiling;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TQueueExportManager
    : public IQueueExportManager
{
public:
    TQueueExportManager(
        IConnectionPtr nativeConnection,
        std::string defaultQueueAgentUser,
        TQueueExportManagerConfigPtr staticConfig,
        TQueueExportManagerDynamicConfigPtr dynamicConfig)
        : Logger(QueueExportManagerLogger())
        , Profiler_(QueueAgentProfiler()
            .WithPrefix("/static_export_manager"))
        , Throttler_(CreateNamedReconfigurableThroughputThrottler(
            TThroughputThrottlerConfig::Create(dynamicConfig->ExportRateLimit),
            /*name*/ "ExportThrottler",
            Logger,
            Profiler_))
        , Config_(std::move(staticConfig))
        , ClientDirectory_(New<TClientDirectory>(
            nativeConnection->GetClusterDirectory(),
            TClientOptions::FromUser(Config_->User.value_or(defaultQueueAgentUser))))
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    IThroughputThrottlerPtr GetExportThrottler() const override
    {
        return Throttler_;
    }

    const TClientDirectoryPtr& GetQueueExportClientDirectory() const override
    {
        return ClientDirectory_;
    }

    void Reconfigure(
        const TQueueExportManagerDynamicConfigPtr& newConfig) override
    {
        auto oldConfig = DynamicConfig_;

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

    const TQueueExportManagerConfigPtr Config_;
    const TClientDirectoryPtr ClientDirectory_;

    TQueueExportManagerDynamicConfigPtr DynamicConfig_;
};

////////////////////////////////////////////////////////////////////////////////

IQueueExportManagerPtr CreateQueueExportManager(
    NApi::NNative::IConnectionPtr nativeConnection,
    std::string defaultQueueAgentUser,
    TQueueExportManagerConfigPtr staticConfig,
    TQueueExportManagerDynamicConfigPtr dynamicConfig)
{
    return New<TQueueExportManager>(
        std::move(nativeConnection),
        std::move(defaultQueueAgentUser),
        std::move(staticConfig),
        std::move(dynamicConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
