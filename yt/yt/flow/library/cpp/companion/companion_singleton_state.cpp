#include "companion_singleton_state.h"

#include "config.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

namespace {

TAtomicIntrusivePtr<TCompanionExecutionConfig>& CompanionRunConfigSingletonState()
{
    static TAtomicIntrusivePtr<TCompanionExecutionConfig> state{New<TCompanionExecutionConfig>()};
    return state;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SetCompanionExecutionConfig(const TCompanionExecutionConfigPtr& companionConfig)
{
    CompanionRunConfigSingletonState().Store(companionConfig);
}

TCompanionExecutionConfigPtr GetCompanionExecutionConfig()
{
    return CompanionRunConfigSingletonState().Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
