#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config);
void ConfigureSingletons(const TDeprecatedSingletonsConfigPtr& config);
void ReconfigureSingletons(
    const TSingletonsConfigPtr& config,
    const TSingletonsDynamicConfigPtr& dynamicConfig);
void ReconfigureSingletons(
    const TDeprecatedSingletonsConfigPtr& config,
    const TDeprecatedSingletonsDynamicConfigPtr& dynamicConfig);
void StartDiagnosticDump(const TDiagnosticDumpConfigPtr& config);
void StartDiagnosticDump(const TDeprecatedDiagnosticDumpConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
