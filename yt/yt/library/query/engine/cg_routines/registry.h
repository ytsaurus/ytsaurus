#pragma once

#include <yt/yt/library/codegen/module.h>
#include <yt/yt/library/codegen/routine_registry.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NCodegen::TRoutineRegistry* GetQueryRoutineRegistry(NCodegen::EExecutionBackend backend);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
