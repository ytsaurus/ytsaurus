#include "counter_manager.h"

#include "private.h"

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TControllerAgentCounterManager::TControllerAgentCounterManager()
{
    for (auto type : TEnumTraits<EOperationType>::GetDomainValues()) {
        AssertionsFailed_[type] = ControllerAgentProfiler
            .WithTag("operation_type", FormatEnum(type))
            .Counter("/assertions_failed"); 
    }
}

TControllerAgentCounterManager* TControllerAgentCounterManager::Get()
{
    return Singleton<TControllerAgentCounterManager>();
}

void TControllerAgentCounterManager::IncrementAssertionsFailed(EOperationType operationType)
{
    AssertionsFailed_[operationType].Increment();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
