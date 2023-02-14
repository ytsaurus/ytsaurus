#pragma once

#include "client.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

void SetCachingHeader(
    const NRpc::IClientRequestPtr& request,
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options,
    NHydra::TRevision refreshRevision = NHydra::NullRevision);

void SetBalancingHeader(
    const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& request,
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
