#include "stdafx.h"
#include "rpc_helpers.h"

#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMutationId GetRpcMutationId(IServiceContextPtr context)
{
    return context->RequestAttributes().Get<TMutationId>("mutation_id", NullMutationId);
}

void GenerateRpcMutationId(IClientRequestPtr request)
{
    SetRpcMutationId(request, TMutationId::Create());
}

void GenerateRpcMutationId(IClientRequestPtr request, const TMutationId& id)
{
    if (id == NullMutationId) {
        SetRpcMutationId(request, TMutationId::Create());
    }
    else {
        SetRpcMutationId(request, id);
    }
}

void SetRpcMutationId(IClientRequestPtr request, const TMutationId& id)
{
    request->MutableAttributes()->Set("mutation_id", id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
