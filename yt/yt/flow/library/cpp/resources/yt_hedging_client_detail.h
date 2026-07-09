#pragma once

#include "yt_hedging_client.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/hedging/hedging.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYTHedgingClientParameters
    : public virtual NYTree::TYsonStruct
{
    NClient::NHedging::NRpc::THedgingClientOptionsPtr HedgingClientConfig;
    std::optional<NClient::NHedging::NRpc::TReplicationLagPenaltyProviderOptionsPtr> PenaltyProviderConfig;
    TString PenaltyProviderClusterUrl;

    REGISTER_YSON_STRUCT(TYTHedgingClientParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TYTHedgingClient
    : public IYTHedgingClient
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYTHedgingClientParameters);

    TYTHedgingClient(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    NApi::IClientPtr Get() override;

private:
    NApi::IClientPtr YtHedgingClient_;
};

DEFINE_REFCOUNTED_TYPE(TYTHedgingClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
