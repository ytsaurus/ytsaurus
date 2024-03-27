#include "protobuf_helpers.h"

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT::NCellMasterClient {

using namespace NApi::NNative;
using namespace NRpc;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TServiceDiscoveryEndpointsConfig* protoServiceDiscoveryEndpointsConfig,
    const TServiceDiscoveryEndpointsConfigPtr& serviceDiscoveryEndpointsConfig)
{
    protoServiceDiscoveryEndpointsConfig->Clear();

    if (serviceDiscoveryEndpointsConfig->Cluster) {
        ToProto(protoServiceDiscoveryEndpointsConfig->mutable_cluster(), *serviceDiscoveryEndpointsConfig->Cluster);
    }
    ToProto(protoServiceDiscoveryEndpointsConfig->mutable_clusters(), serviceDiscoveryEndpointsConfig->Clusters);
    ToProto(protoServiceDiscoveryEndpointsConfig->mutable_endpoint_set_id(), serviceDiscoveryEndpointsConfig->EndpointSetId);
    protoServiceDiscoveryEndpointsConfig->set_update_period(ToProto<i64>(serviceDiscoveryEndpointsConfig->UpdatePeriod));
}

void FromProto(
    TServiceDiscoveryEndpointsConfigPtr* serviceDiscoveryEndpointsConfig,
    const NProto::TServiceDiscoveryEndpointsConfig& protoServiceDiscoveryEndpointsConfig)
{
    auto config = New<TServiceDiscoveryEndpointsConfig>();
    if (protoServiceDiscoveryEndpointsConfig.has_cluster()) {
        config->Cluster = FromProto<TString>(protoServiceDiscoveryEndpointsConfig.cluster());
    }
    FromProto(&config->Clusters, protoServiceDiscoveryEndpointsConfig.clusters());
    FromProto(&config->EndpointSetId, protoServiceDiscoveryEndpointsConfig.endpoint_set_id());
    FromProto(&config->UpdatePeriod, protoServiceDiscoveryEndpointsConfig.update_period());

    *serviceDiscoveryEndpointsConfig = std::move(config);
}

void ToProto(
    NProto::TCellDirectoryItem* protoCellDirectoryItem,
    const TMasterConnectionConfigPtr& masterConnectionConfig)
{
    protoCellDirectoryItem->Clear();

    protoCellDirectoryItem->set_rpc_timeout(ToProto<i64>(masterConnectionConfig->RpcTimeout));
    ToProto(protoCellDirectoryItem->mutable_cell_id(), masterConnectionConfig->CellId);
    protoCellDirectoryItem->set_ignore_peer_state(masterConnectionConfig->IgnorePeerState);
    if (masterConnectionConfig->Addresses) {
        ToProto(protoCellDirectoryItem->mutable_addresses(), *masterConnectionConfig->Addresses);
    }
    protoCellDirectoryItem->set_disable_balancing_on_single_address(masterConnectionConfig->DisableBalancingOnSingleAddress);
    if (masterConnectionConfig->Endpoints) {
        ToProto(protoCellDirectoryItem->mutable_endpoints(), masterConnectionConfig->Endpoints);
    }
    if (masterConnectionConfig->HedgingDelay) {
        protoCellDirectoryItem->set_hedging_delay(ToProto<i64>(*masterConnectionConfig->HedgingDelay));
    }
    protoCellDirectoryItem->set_cancel_primary_request_on_hedging(masterConnectionConfig->CancelPrimaryRequestOnHedging);
    protoCellDirectoryItem->set_max_concurrent_discover_requests(masterConnectionConfig->MaxConcurrentDiscoverRequests);
    protoCellDirectoryItem->set_random_peer_eviction_period(ToProto<i64>(masterConnectionConfig->RandomPeerEvictionPeriod));
    protoCellDirectoryItem->set_enable_peer_polling(masterConnectionConfig->EnablePeerPolling);
    protoCellDirectoryItem->set_peer_polling_period(ToProto<i64>(masterConnectionConfig->PeerPollingPeriod));
    protoCellDirectoryItem->set_peer_polling_period_splay(ToProto<i64>(masterConnectionConfig->PeerPollingPeriodSplay));
    protoCellDirectoryItem->set_peer_polling_request_timeout(ToProto<i64>(masterConnectionConfig->PeerPollingRequestTimeout));
    protoCellDirectoryItem->set_discovery_session_timeout(ToProto<i64>(masterConnectionConfig->DiscoverySessionTimeout));
    protoCellDirectoryItem->set_max_peer_count(masterConnectionConfig->MaxPeerCount);
    protoCellDirectoryItem->set_hashes_per_peer(masterConnectionConfig->HashesPerPeer);
    protoCellDirectoryItem->set_peer_priority_strategy(static_cast<i64>(masterConnectionConfig->PeerPriorityStrategy));
    protoCellDirectoryItem->set_min_peer_count_for_priority_awareness(masterConnectionConfig->MinPeerCountForPriorityAwareness);
    protoCellDirectoryItem->set_enable_power_of_two_choices_strategy(masterConnectionConfig->EnablePowerOfTwoChoicesStrategy);
    protoCellDirectoryItem->set_discover_timeout(ToProto<i64>(masterConnectionConfig->DiscoverTimeout));
    protoCellDirectoryItem->set_acknowledgement_timeout(ToProto<i64>(masterConnectionConfig->AcknowledgementTimeout));
    protoCellDirectoryItem->set_rediscover_period(ToProto<i64>(masterConnectionConfig->RediscoverPeriod));
    protoCellDirectoryItem->set_rediscover_splay(ToProto<i64>(masterConnectionConfig->RediscoverSplay));
    protoCellDirectoryItem->set_hard_backoff_time(ToProto<i64>(masterConnectionConfig->HardBackoffTime));
    protoCellDirectoryItem->set_soft_backoff_time(ToProto<i64>(masterConnectionConfig->SoftBackoffTime));
    protoCellDirectoryItem->set_retry_backoff_time(ToProto<i64>(masterConnectionConfig->RetryBackoffTime));
    protoCellDirectoryItem->set_retry_attempts(masterConnectionConfig->RetryAttempts);
    if (masterConnectionConfig->RetryTimeout) {
        protoCellDirectoryItem->set_retry_timeout(ToProto<i64>(*masterConnectionConfig->RetryTimeout));
    }
}

void FromProto(
    TMasterConnectionConfigPtr* masterConnectionConfig,
    const NProto::TCellDirectoryItem& protoCellDirectoryItem)
{
    auto config = New<TMasterConnectionConfig>();

    FromProto(&config->RpcTimeout, protoCellDirectoryItem.rpc_timeout());
    FromProto(&config->CellId, protoCellDirectoryItem.cell_id());
    FromProto(&config->IgnorePeerState, protoCellDirectoryItem.ignore_peer_state());
    config->Addresses = FromProto<std::vector<TString>>(protoCellDirectoryItem.addresses());
    FromProto(&config->DisableBalancingOnSingleAddress, protoCellDirectoryItem.disable_balancing_on_single_address());
    FromProto(&config->Endpoints, protoCellDirectoryItem.endpoints());
    if (protoCellDirectoryItem.has_hedging_delay()) {
        config->HedgingDelay = FromProto<TDuration>(protoCellDirectoryItem.hedging_delay());
    }
    FromProto(&config->CancelPrimaryRequestOnHedging, protoCellDirectoryItem.cancel_primary_request_on_hedging());
    FromProto(&config->MaxConcurrentDiscoverRequests, protoCellDirectoryItem.max_concurrent_discover_requests());
    FromProto(&config->RandomPeerEvictionPeriod, protoCellDirectoryItem.random_peer_eviction_period());
    FromProto(&config->EnablePeerPolling, protoCellDirectoryItem.enable_peer_polling());
    FromProto(&config->PeerPollingPeriod, protoCellDirectoryItem.peer_polling_period());
    FromProto(&config->PeerPollingPeriodSplay, protoCellDirectoryItem.peer_polling_period_splay());
    FromProto(&config->PeerPollingRequestTimeout, protoCellDirectoryItem.peer_polling_request_timeout());
    FromProto(&config->DiscoverySessionTimeout, protoCellDirectoryItem.discovery_session_timeout());
    FromProto(&config->MaxPeerCount, protoCellDirectoryItem.max_peer_count());
    FromProto(&config->HashesPerPeer, protoCellDirectoryItem.hashes_per_peer());
    config->PeerPriorityStrategy = CheckedEnumCast<EPeerPriorityStrategy>(protoCellDirectoryItem.peer_priority_strategy());
    FromProto(&config->MinPeerCountForPriorityAwareness, protoCellDirectoryItem.min_peer_count_for_priority_awareness());
    FromProto(&config->EnablePowerOfTwoChoicesStrategy, protoCellDirectoryItem.enable_power_of_two_choices_strategy());
    FromProto(&config->DiscoverTimeout, protoCellDirectoryItem.discover_timeout());
    FromProto(&config->AcknowledgementTimeout, protoCellDirectoryItem.acknowledgement_timeout());
    FromProto(&config->RediscoverPeriod, protoCellDirectoryItem.rediscover_period());
    FromProto(&config->RediscoverSplay, protoCellDirectoryItem.rediscover_splay());
    FromProto(&config->HardBackoffTime, protoCellDirectoryItem.hard_backoff_time());
    FromProto(&config->SoftBackoffTime, protoCellDirectoryItem.soft_backoff_time());
    FromProto(&config->RetryBackoffTime, protoCellDirectoryItem.retry_backoff_time());
    FromProto(&config->RetryAttempts, protoCellDirectoryItem.retry_attempts());
    if (protoCellDirectoryItem.has_retry_timeout()) {
        config->RetryTimeout = FromProto<TDuration>(protoCellDirectoryItem.retry_timeout());
    }

    *masterConnectionConfig = std::move(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
