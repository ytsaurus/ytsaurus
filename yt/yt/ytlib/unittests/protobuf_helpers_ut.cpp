#include <gtest/gtest.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/cell_master_client/protobuf_helpers.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NCellMasterClient {
namespace {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TEST(TParsing, ServiceDiscoveryEndpointsConfig)
{
    NProto::TServiceDiscoveryEndpointsConfig expectedEndpointsProto;
    *expectedEndpointsProto.add_clusters() = "aba";
    *expectedEndpointsProto.add_clusters() = "caba";
    expectedEndpointsProto.set_endpoint_set_id("a-b-c-d");
    expectedEndpointsProto.set_update_period(21);

    NProto::TServiceDiscoveryEndpointsConfig resultedEndpointsProto;
    TServiceDiscoveryEndpointsConfigPtr parsedEndpoints;
    FromProto(&parsedEndpoints, expectedEndpointsProto);
    ToProto(&resultedEndpointsProto, parsedEndpoints);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(resultedEndpointsProto, expectedEndpointsProto));
}

TEST(TParsing, CellDirectoryItem)
{
    NProto::TServiceDiscoveryEndpointsConfig endpoints;
    *endpoints.add_clusters() = "aba";
    *endpoints.add_clusters() = "caba";
    endpoints.set_endpoint_set_id("a-b-c-d");
    endpoints.set_update_period(21);

    NProto::TCellDirectoryItem expectedConfigProto;
    expectedConfigProto.set_rpc_timeout(128);
    ToProto(expectedConfigProto.mutable_cell_id(), NObjectClient::TCellId(42, 43, 44, 45));
    expectedConfigProto.set_ignore_peer_state(true);
    *expectedConfigProto.add_addresses() = "localhost:4242";
    *expectedConfigProto.add_addresses() = "localhost:4343";
    expectedConfigProto.set_disable_balancing_on_single_address(false);
    expectedConfigProto.set_hedging_delay(23);
    expectedConfigProto.set_cancel_primary_request_on_hedging(true);
    expectedConfigProto.set_max_concurrent_discover_requests(33);
    expectedConfigProto.set_random_peer_eviction_period(20);
    expectedConfigProto.set_enable_peer_polling(false);
    expectedConfigProto.set_peer_polling_period(111);
    expectedConfigProto.set_peer_polling_period_splay(8);
    expectedConfigProto.set_peer_polling_request_timeout(99);
    expectedConfigProto.set_discovery_session_timeout(134);
    expectedConfigProto.set_max_peer_count(81);
    expectedConfigProto.set_hashes_per_peer(22);
    expectedConfigProto.set_peer_priority_strategy(1);
    expectedConfigProto.set_min_peer_count_for_priority_awareness(5);
    expectedConfigProto.set_enable_power_of_two_choices_strategy(true);
    expectedConfigProto.set_discover_timeout(707);
    expectedConfigProto.set_acknowledgement_timeout(82);
    expectedConfigProto.set_rediscover_period(31);
    expectedConfigProto.set_rediscover_splay(53);
    expectedConfigProto.set_hard_backoff_time(47);
    expectedConfigProto.set_soft_backoff_time(68);
    expectedConfigProto.set_retry_backoff_time(27);
    expectedConfigProto.set_retry_attempts(35);
    expectedConfigProto.set_retry_timeout(95);

    {
        NProto::TCellDirectoryItem resultedConfigProto;
        NApi::NNative::TMasterConnectionConfigPtr parsedConfig;
        FromProto(&parsedConfig, expectedConfigProto);
        ToProto(&resultedConfigProto, parsedConfig);
        EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(resultedConfigProto, expectedConfigProto));
    }

    {
        NProto::TCellDirectoryItem resultedConfigProto;
        NApi::NNative::TMasterConnectionConfigPtr parsedConfig;
        expectedConfigProto.clear_addresses();
        *expectedConfigProto.mutable_endpoints() = std::move(endpoints);
        FromProto(&parsedConfig, expectedConfigProto);
        ToProto(&resultedConfigProto, parsedConfig);
        EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(resultedConfigProto, expectedConfigProto));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellMasterClient
