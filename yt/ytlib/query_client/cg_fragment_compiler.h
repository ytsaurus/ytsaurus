#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<
    llvm::Function*(
        const TPlanFragmentPtr&,
        const TCGFragment&,
        const TCGBinding&)
    > TCGFragmentCompiler;

TCGFragmentCompiler CreateFragmentCompiler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

