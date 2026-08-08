#include "porto_tracker.h"

#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsPortoAvailable(const TLogger& logger)
{
    const TLogger& Logger = logger;

    // Porto answers only through its socket, so the environment cannot be told apart
    // by env variables alone; ask porto for the container we run in.
    try {
        auto executor = NContainers::CreatePortoExecutor(
            New<NContainers::TPortoExecutorDynamicConfig>(),
            "porto-probe");
        auto self = NContainers::GetSelfPortoInstance(executor);
        YT_TLOG_INFO("Porto is available")
            .With("Container", self->GetName());
        return true;
    } catch (const std::exception& ex) {
        YT_TLOG_DEBUG("Porto is not available")
            .With(TError(ex));
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TryEnablePortoResourceTracker(std::optional<double> vcpuFactor, const TLogger& logger)
{
    if (!IsPortoAvailable(logger)) {
        return;
    }

    // Porto emits the vcpu sensors only when the factor is set; #GetNodeInfo() is its only source.
    auto podSpec = New<NContainers::TPodSpecConfig>();
    podSpec->CpuToVCpuFactor = vcpuFactor;
    NContainers::EnablePortoResourceTracker(podSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
