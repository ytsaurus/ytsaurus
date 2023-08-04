#pragma once

#include "functions_builtin_registry.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFunctionRegistryBuilder> CreateTypeInferrerFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
