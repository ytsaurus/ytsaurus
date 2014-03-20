#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TPlanFragment;
class TCGFragment;
struct TFragmentParams;

typedef std::function<
    llvm::Function*(
        const TPlanFragment&,
        const TCGFragment&,
        const TCGBinding&)
    > TCGFragmentCompiler;

TCGFragmentCompiler CreateFragmentCompiler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

