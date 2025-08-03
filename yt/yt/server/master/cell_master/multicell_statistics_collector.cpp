#include "multicell_statistics_collector_detail.h"

#include "config_manager.h"
#include "multicell_node_statistics.h"

#include <yt/yt/server/master/chunk_server/lost_vital_chunks_sample.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// Snapshot format of collector is order sensitive, append only.
// Appended values must ensure snapshot compatibility in their loaders i.e. be
// noop before snapshot reign is promoted
using TMulticellStatisticsCollectorImpl = TMulticellStatisticsCollectorCommon<
    TMulticellNodeStatistics,
    NChunkServer::TLostVitalChunksSample
>;

////////////////////////////////////////////////////////////////////////////////

class TMulticellStatisticsCollector
    : public TMulticellStatisticsCollectorImpl
{
public:
    explicit TMulticellStatisticsCollector(TBootstrap* bootstrap)
        : TMulticellStatisticsCollectorImpl(bootstrap)
    {
        RegisterMutationHandlers();

        // COMPAT(koloshmet): remove when 25.1 is deployed.
        RegisterLoader(
            "TMulticellStatisticsCollector.Values",
            BIND_NO_PROPAGATE(&TMulticellStatisticsCollector::LoadValues, Unretained(this)));

        RegisterLoader(
            "MulticellStatisticsCollector.Values",
            BIND_NO_PROPAGATE(&TMulticellStatisticsCollector::LoadValues, Unretained(this)));

        RegisterSaver(
            NHydra::ESyncSerializationPriority::Values,
            "MulticellStatisticsCollector.Values",
            BIND_NO_PROPAGATE(&TMulticellStatisticsCollector::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND_NO_PROPAGATE(&TMulticellStatisticsCollector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual const TMulticellNodeStatistics& GetMulticellNodeStatistics() override
    {
        return GetValue<TMulticellNodeStatistics>();
    }
    virtual TMulticellNodeStatistics& GetMutableMulticellNodeStatistics() override
    {
        return GetValue<TMulticellNodeStatistics>();
    }

    virtual const NChunkServer::TLostVitalChunksSample& GetLostVitalChunksSample() override
    {
        return GetValue<NChunkServer::TLostVitalChunksSample>();
    }
    virtual NChunkServer::TLostVitalChunksSample& GetMutableLostVitalChunksSample() override
    {
        return GetValue<NChunkServer::TLostVitalChunksSample>();
    }
};

////////////////////////////////////////////////////////////////////////////////

IMulticellStatisticsCollectorPtr CreateMulticellStatisticsCollector(TBootstrap* bootstrap)
{
    return New<TMulticellStatisticsCollector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
