#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCGFragment;

class TCGCache
{
public:
    TCGCache();
    ~TCGCache();

    typedef std::function<llvm::Function*(const TPlanFragment&, const TCGImmediates&, TCGFragment&)> TCompiler;

    std::pair<TCodegenedFunction, TFragmentParams> Codegen(
        const TPlanFragment& fragment,
        TCompiler compiler);

private:
    class TCachedCGFragment;

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

