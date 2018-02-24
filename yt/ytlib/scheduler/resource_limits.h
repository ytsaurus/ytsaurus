#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimits
{
    TNullable<int> UserSlots;
    TNullable<double> Cpu;
    TNullable<int> Network;
    TNullable<i64> Memory;
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TResourceLimits* protoResourceLimits, const NScheduler::TResourceLimits& resourceLimits);
void FromProto(NScheduler::TResourceLimits& resourceLimits, const NProto::TResourceLimits& protoResourceLimits);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
