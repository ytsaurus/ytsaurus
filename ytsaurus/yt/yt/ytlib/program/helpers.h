#pragma once

#include <yt/yt/library/program/helpers.h>

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureNativeSingletons(const TNativeSingletonsConfigPtr& config);

void ReconfigureNativeSingletons(
    const TNativeSingletonsConfigPtr& config,
    const TNativeSingletonsDynamicConfigPtr& dynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
