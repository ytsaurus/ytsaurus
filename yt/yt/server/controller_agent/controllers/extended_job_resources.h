#pragma once

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TExtendedJobResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY(i64, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(NVectorHdrf::TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(int, Gpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, JobProxyMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, JobProxyMemoryWithFixedWriteBufferSize);
    DEFINE_BYVAL_RW_PROPERTY(i64, UserJobMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, FootprintMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, Network);

    void SetCpu(double cpu)
    {
        Cpu_ = NVectorHdrf::TCpuResource(cpu);
    }

public:
    i64 GetMemory() const;

    PHOENIX_DECLARE_TYPE(TExtendedJobResources, 0x3c8e4f12);
};

void Serialize(const TExtendedJobResources& resources, NYson::IYsonConsumer* consumer);
TString FormatResources(const TExtendedJobResources& resources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
