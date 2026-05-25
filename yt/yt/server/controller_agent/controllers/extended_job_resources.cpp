#include "extended_job_resources.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

i64 TExtendedJobResources::GetMemory() const
{
    return JobProxyMemory_ + UserJobMemory_ + FootprintMemory_;
}

void TExtendedJobResources::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Cpu_);
    PHOENIX_REGISTER_FIELD(2, Gpu_);
    PHOENIX_REGISTER_FIELD(3, UserSlots_);
    PHOENIX_REGISTER_FIELD(4, JobProxyMemory_);
    PHOENIX_REGISTER_FIELD(5, JobProxyMemoryWithFixedWriteBufferSize_,
        .SinceVersion(ESnapshotVersion::TableWriteBufferEstimation));
    PHOENIX_REGISTER_FIELD(6, UserJobMemory_);
    PHOENIX_REGISTER_FIELD(7, FootprintMemory_);
    PHOENIX_REGISTER_FIELD(8, Network_);
}

PHOENIX_DEFINE_TYPE(TExtendedJobResources);

void Serialize(const TExtendedJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cpu").Value(resources.GetCpu())
            .Item("gpu").Value(resources.GetGpu())
            .Item("user_slots").Value(resources.GetUserSlots())
            .Item("job_proxy_memory").Value(resources.GetJobProxyMemory())
            .Item("job_proxy_memory_with_fixed_write_buffer_size").Value(resources.GetJobProxyMemoryWithFixedWriteBufferSize())
            .Item("user_job_memory").Value(resources.GetUserJobMemory())
            .Item("footprint_memory").Value(resources.GetFootprintMemory())
            .Item("network").Value(resources.GetNetwork())
        .EndMap();
}

TString FormatResources(const TExtendedJobResources& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, JobProxyMemory: %vMB, "
        "JobProxyMemoryWithFixedWriteBufferSize: %vMB, UserJobMemory: %vMB, FootprintMemory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetJobProxyMemory() / 1_MB,
        resources.GetJobProxyMemoryWithFixedWriteBufferSize() / 1_MB,
        resources.GetUserJobMemory() / 1_MB,
        resources.GetFootprintMemory() / 1_MB,
        resources.GetNetwork());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
