#include "counter_manager.h"

#include "private.h"

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TControllerAgentCounterManager::TControllerAgentCounterManager()
{
    static const TEnumMemberTagCache<EOperationType> OperationTypeTagCache("operation_type");
    for (auto type : TEnumTraits<EOperationType>::GetDomainValues()) {
        AssertionsFailed_[type] = TMonotonicCounter(
            "/assertions_failed",
            {OperationTypeTagCache.GetTag(type)});
    }
}

TControllerAgentCounterManager* TControllerAgentCounterManager::Get()
{
    return Singleton<TControllerAgentCounterManager>();
}

void TControllerAgentCounterManager::IncrementAssertionsFailed(EOperationType operationType)
{
    ControllerAgentProfiler.Increment(AssertionsFailed_[operationType]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
