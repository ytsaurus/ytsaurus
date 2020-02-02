#pragma once

#include "client.h"

#include <yt/core/rpc/public.h>

#include <yt/client/hydra/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

void SetCachingHeader(
    const NRpc::IClientRequestPtr& request,
    const TConnectionConfigPtr& config,
    const TMasterReadOptions& options,
    NHydra::TRevision refreshRevision = NHydra::NullRevision);

void SetBalancingHeader(
    const NRpc::IClientRequestPtr& request,
    const TConnectionConfigPtr& config,
    const TMasterReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
