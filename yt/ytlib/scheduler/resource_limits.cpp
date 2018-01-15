#include "resource_limits.h"

#include <yt/ytlib/scheduler/proto/scheduler_service.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TResourceLimits* protoResourceLimits, const NScheduler::TResourceLimits& resourceLimits)
{
#define XX(Resource, ProtoResource) if (resourceLimits.Resource) { \
    protoResourceLimits->set_ ## ProtoResource (*resourceLimits.Resource); \
}
    XX(Cpu, cpu)
    XX(UserSlots, user_slots)
    XX(Memory, memory)
    XX(Network, network)
#undef XX
}

void FromProto(NScheduler::TResourceLimits& resourceLimits, const NProto::TResourceLimits& protoResourceLimits)
{
#define XX(Resource, ProtoResource) if (protoResourceLimits.has_ ## ProtoResource()) { \
    resourceLimits.Resource = protoResourceLimits.ProtoResource(); \
}
    XX(Cpu, cpu)
    XX(UserSlots, user_slots)
    XX(Memory, memory)
    XX(Network, network)
#undef XX
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
