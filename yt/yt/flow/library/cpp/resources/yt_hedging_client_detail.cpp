#include "yt_hedging_client_detail.h"
#include "yt_client_factory.h"

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NClient::NHedging::NRpc;

////////////////////////////////////////////////////////////////////////////////

void TYTHedgingClientParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("hedging_client_config", &TThis::HedgingClientConfig)
        .IsRequired();
    registrar.Parameter("penalty_provider_config", &TThis::PenaltyProviderConfig)
        .Optional();
    registrar.Parameter("penalty_provider_cluster_url", &TThis::PenaltyProviderClusterUrl)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TYTHedgingClient::TYTHedgingClient(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : IYTHedgingClient(std::move(context), std::move(dynamicContext))
{ }

TFuture<void> TYTHedgingClient::Load(const THashMap<TResourceId, IResourcePtr>& dependencies)
{
    auto clientFactory = GetOrCrash(dependencies, YTClientFactoryDefaultResourceId)
        ->As<IYTClientFactory>();

    IPenaltyProviderPtr penaltyProvider;
    if (GetParameters()->PenaltyProviderConfig) {
        auto masterClient = clientFactory->GetClient(GetParameters()->PenaltyProviderClusterUrl);
        penaltyProvider = CreateReplicationLagPenaltyProvider(
            *GetParameters()->PenaltyProviderConfig,
            masterClient);
    } else {
        penaltyProvider = CreateDummyPenaltyProvider();
    }

    YtHedgingClient_ = CreateHedgingClient(
        GetParameters()->HedgingClientConfig,
        clientFactory,
        penaltyProvider);

    return OKFuture;
}

IClientPtr TYTHedgingClient::Get()
{
    return YtHedgingClient_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
