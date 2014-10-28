#include "stdafx.h"
#include "rpc_helpers.h"
#include "mutation_context.h"

#include <core/rpc/service.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCallback<void(const TErrorOr<TMutationResponse>&)> CreateRpcResponseHandler(IServiceContextPtr context)
{
    return BIND([=] (const TErrorOr<TMutationResponse>& result) {
        if (result.IsOK()) {
            const auto& response = result.Value();
            if (response.Data) {
                context->Reply(response.Data);
            } else {
                context->Reply(TError());
            }
        } else {
            context->Reply(TError(result));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
