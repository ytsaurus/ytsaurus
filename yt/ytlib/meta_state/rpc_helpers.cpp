#include "stdafx.h"
#include "rpc_helpers.h"

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCallback<void (const TError& error)> CreateRpcErrorHandler(IServiceContextPtr context)
{
    return BIND([=] (const TError& error) {
        context->Reply(TError(
            EErrorCode::Unavailable,
            Sprintf("Error committing mutations\n%s", error.ToString())));
    });
}

TClosure CreateRpcSuccessHandler(IServiceContextPtr context)
{
    return BIND([=] () {
        context->Reply(TError());
    });
}

TMutationId GetRpcMutationId(IServiceContextPtr context)
{
    return context->RequestAttributes().Get<TMutationId>("mutation_id", NullMutationId);
}

void GenerateRpcMutationId(IClientRequestPtr request)
{
    SetRpcMutationId(request, TMutationId::Create());
}

void SetRpcMutationId(IClientRequestPtr request, const TMutationId& id)
{
    request->Attributes().Set("mutation_id", id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
