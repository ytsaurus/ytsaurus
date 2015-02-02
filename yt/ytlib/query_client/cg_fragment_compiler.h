#pragma once

#include "cg_types.h"

#include <core/codegen/module.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    const TConstQueryPtr& query,
    const TCGBinding& binding);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

