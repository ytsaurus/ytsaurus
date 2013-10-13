#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TClosure CreateRpcSuccessHandler(NRpc::IServiceContextPtr context);
TCallback<void (const TError& error)> CreateRpcErrorHandler(NRpc::IServiceContextPtr context);

NHydra::TMutationId GenerateMutationId();

TMutationId GetMutationId(NRpc::IServiceContextPtr context);
TMutationId GetMutationId(const NRpc::NProto::TRequestHeader& header);

void GenerateMutationId(NRpc::IClientRequestPtr request);
void SetMutationId(NRpc::NProto::TRequestHeader* header, const TMutationId& id);
void SetMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);
void SetOrGenerateMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
