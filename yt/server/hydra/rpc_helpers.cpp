#include "stdafx.h"
#include "rpc_helpers.h"
#include "mutation_context.h"

#include <core/rpc/service.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCallback<void(const TMutationResponse&)> CreateRpcSuccessHandler(IServiceContextPtr context)
{
    return BIND([=] (const TMutationResponse& response) {
        if (response.Data) {
            context->Reply(response.Data);
        } else {
            context->Reply(TError());
        }
    });
}

TCallback<void(const TError&)> CreateRpcErrorHandler(IServiceContextPtr context)
{
    return BIND([=] (const TError& error) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Error committing mutations")
            << error);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
