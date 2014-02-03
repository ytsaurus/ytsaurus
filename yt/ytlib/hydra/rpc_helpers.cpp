#include "stdafx.h"
#include "rpc_helpers.h"

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/client.h>
#include <core/rpc/service.h>

#include <ytlib/hydra/rpc_helpers.pb.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

TMutationId GenerateMutationId()
{
    return TMutationId::Create();
}

TMutationId GetMutationId(const TRequestHeader& header)
{
    return header.HasExtension(TMutatingExt::mutation_id)
           ? FromProto<TMutationId>(header.GetExtension(TMutatingExt::mutation_id))
           : NullMutationId;
}

TMutationId GetMutationId(IServiceContextPtr context)
{
    return GetMutationId(context->RequestHeader());
}

void GenerateMutationId(IClientRequestPtr request)
{
    SetMutationId(request, GenerateMutationId());
}

void SetMutationId(TRequestHeader* header, const TMutationId& id)
{
    ToProto(header->MutableExtension(TMutatingExt::mutation_id), id);
}

void SetMutationId(IClientRequestPtr request, const TMutationId& id)
{
    SetMutationId(&request->Header(), id);
}

void SetOrGenerateMutationId(IClientRequestPtr request, const TMutationId& id)
{
    SetMutationId(request, id == NullMutationId ? TMutationId::Create() : id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
