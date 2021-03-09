#include "job_resources_serialize.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TExtendedJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cpu").Value(resources.GetCpu())
            .Item("gpu").Value(resources.GetGpu())
            .Item("user_slots").Value(resources.GetUserSlots())
            .Item("job_proxy_memory").Value(resources.GetJobProxyMemory())
            .Item("user_job_memory").Value(resources.GetUserJobMemory())
            .Item("footprint_memory").Value(resources.GetFootprintMemory())
            .Item("network").Value(resources.GetNetwork())
        .EndMap();
}

void Serialize(const TJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
    #define XX(name, Name) .Item(#name).Value(resources.Get##Name())
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
            // COMPAT(psushin): fix for a web-face.
            .Item("memory").Value(resources.GetMemory())
        .EndMap();
}

void Deserialize(TJobResources& resources, INodePtr node)
{
    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = resources.Get##Name(); \
            Deserialize(value, child); \
            resources.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
