#pragma once

#include "public.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void SetCompanionExecutionConfig(const TCompanionExecutionConfigPtr& companionConfig);

TCompanionExecutionConfigPtr GetCompanionExecutionConfig();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
