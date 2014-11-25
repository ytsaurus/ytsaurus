#pragma once

#include "cg_types.h"

#include <core/codegen/module.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<
    TCGQueryCallback (
        const TConstQueryPtr&,
        const TCGBinding&)
    > TCGFragmentCompiler;

TCGFragmentCompiler CreateFragmentCompiler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

