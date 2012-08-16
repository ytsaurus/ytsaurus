#ifndef RPC_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include rpc-helpers.h"
#endif

#include <ytlib/rpc/error.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TContext>
TClosure CreateRpcSuccessHandler(TIntrusivePtr<TContext> context)
{
    return BIND([=] () {
        context->Reply(TError());
    });
}

template <class TContext>
TCallback<void (const TError& error)> CreateRpcErrorHandler(TIntrusivePtr<TContext> context)
{
    return BIND([=] (const TError& error) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            Sprintf("Error committing mutations\n%s", ~error.ToString())));
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
