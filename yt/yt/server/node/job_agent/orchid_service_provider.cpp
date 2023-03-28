#include "orchid_service_provider.h"

#include "job_resource_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/job_controller.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_controller.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NJobAgent {

using namespace NYson;
using namespace NYTree;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProvider
    : public IOrchidServiceProvider
{
public:
    TOrchidServiceProvider(
        IBootstrapBase* bootstrap)
        : Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    }

    IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND_NO_PROPAGATE(&TOrchidServiceProvider::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetJobInvoker());
    }

    void Initialize() override
    { }

private:
    NClusterNode::IBootstrapBase* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("resource_limits").Value(Bootstrap_->GetJobResourceManager()->GetResourceLimits())
                .Item("resource_usage").Value(Bootstrap_->GetJobResourceManager()->GetResourceUsage())
                .Item("active_job_count").BeginMap()
                    .DoIf(Bootstrap_->IsExecNode(), [&] (auto fluent) {
                        fluent.Item(FormatEnum(EJobOrigin::Scheduler))
                            .Value(Bootstrap_->GetExecNodeBootstrap()->GetJobController()->GetActiveJobCount());
                    })
                    .DoIf(Bootstrap_->IsDataNode(), [&] (auto fluent) {
                        fluent.Item(FormatEnum(EJobOrigin::Master))
                            .Value(Bootstrap_->GetDataNodeBootstrap()->GetJobController()->GetActiveJobCount());
                    })
                .EndMap()
                .Item("active_jobs").BeginMap()
                    .DoIf(Bootstrap_->IsExecNode(), [&] (auto fluent) {
                        fluent.Item(FormatEnum(EJobOrigin::Scheduler))
                            .Do(
                                std::bind(
                                    &NExecNode::IJobController::BuildJobsInfo,
                                    Bootstrap_->GetExecNodeBootstrap()->GetJobController(),
                                    std::placeholders::_1));
                    })
                    .DoIf(Bootstrap_->IsDataNode(), [&] (auto fluent) {
                        fluent.Item(FormatEnum(EJobOrigin::Master))
                            .Do(
                                std::bind(
                                    &NDataNode::IJobController::BuildJobsInfo,
                                    Bootstrap_->GetDataNodeBootstrap()->GetJobController(),
                                    std::placeholders::_1));
                    })
                .EndMap()
                .DoIf(Bootstrap_->IsExecNode(), [&] (auto fluent) {
                    const auto& execNodeBootstrap = Bootstrap_->GetExecNodeBootstrap();

                    const auto& jobController = execNodeBootstrap->GetJobController();
                    jobController->BuildJobControllerInfo(fluent);

                    // TODO(pogorelov): Move to BuildJobControllerInfo
                    fluent
                        .Item("gpu_utilization").DoMapFor(
                            execNodeBootstrap->GetGpuManager()->GetGpuInfoMap(),
                            [&] (TFluentMap fluent, const auto& pair) {
                                const auto& [_, gpuInfo] = pair;
                                fluent.Item(ToString(gpuInfo.Index))
                                    .BeginMap()
                                        .Item("update_time").Value(gpuInfo.UpdateTime)
                                        .Item("utilization_gpu_rate").Value(gpuInfo.UtilizationGpuRate)
                                        .Item("utilization_memory_rate").Value(gpuInfo.UtilizationMemoryRate)
                                        .Item("memory_used").Value(gpuInfo.MemoryUsed)
                                        .Item("memory_limit").Value(gpuInfo.MemoryTotal)
                                        .Item("power_used").Value(gpuInfo.PowerDraw)
                                        .Item("power_limit").Value(gpuInfo.PowerLimit)
                                        .Item("clocks_sm_used").Value(gpuInfo.ClocksSm)
                                        .Item("clocks_sm_limit").Value(gpuInfo.ClocksMaxSm)
                                        .Item("sm_utilization_rate").Value(gpuInfo.SMUtilizationRate)
                                        .Item("sm_occupancy_rate").Value(gpuInfo.SMOccupancyRate)
                                    .EndMap();
                            })
                        .Item("slot_manager").DoMap(std::bind(
                            &NExecNode::TSlotManager::BuildOrchidYson,
                            Bootstrap_->GetExecNodeBootstrap()->GetSlotManager(),
                            std::placeholders::_1))
                        .Item("job_proxy_build").Do(
                            std::bind(
                                &NExecNode::IJobController::BuildJobProxyBuildInfo,
                                Bootstrap_->GetExecNodeBootstrap()->GetJobController(),
                                std::placeholders::_1));
                })
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IOrchidServiceProviderPtr CreateOrchidServiceProvider(IBootstrapBase* bootstrap)
{
    return New<TOrchidServiceProvider>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
