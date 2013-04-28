#include "stdafx.h"
#include "rpc_helpers.h"

#include <ytlib/ytree/attribute_helpers.h>
#include <ytlib/ytree/convert.h>

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMutationId GenerateMutationId()
{
    return TMutationId::Create();
}

TMutationId GetMutationId(IServiceContextPtr context)
{
    return context->RequestAttributes().Get<TMutationId>("mutation_id", NullMutationId);
}

TMutationId GetMutationId(const NRpc::NProto::TRequestHeader& header)
{
    FOREACH (const auto& attribute, header.attributes().attributes()) {
        if (attribute.key() == "mutation_id") {
            return ConvertTo<TMutationId>(TYsonString(attribute.value()));
        }
    }
    return NullMutationId;
}

void GenerateMutationId(IClientRequestPtr request)
{
    SetMutationId(request, GenerateMutationId());
}

void SetMutationId(IClientRequestPtr request, const TMutationId& id)
{
    request->MutableAttributes()->Set("mutation_id", id);
}

void SetOrGenerateMutationId(IClientRequestPtr request, const TMutationId& id)
{
    if (id == NullMutationId) {
        SetMutationId(request, TMutationId::Create());
    } else {
        SetMutationId(request, id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
