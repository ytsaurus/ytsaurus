#pragma once

#include "public.h"

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

    std::pair<TCodegenedFunction, TFragmentParams> Codegen(
        const TPlanFragment& fragment,
        std::function<void(const TPlanFragment&, TCGFragment&, const TFragmentParams&)> compiler);

private:
    class TCachedCGFragment;

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

