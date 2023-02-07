#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>

using namespace NYT;
using namespace NYT::NApi;
using namespace NYT::NLogging;
using namespace NYT::NNet;

IClientPtr CreateTestRpcClient()
{
    auto connectionConfig = New<NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    connectionConfig->ClusterUrl = GetEnv("YT_PROXY");

    TClientOptions clientOptions;
    auto connection = NRpcProxy::CreateConnection(connectionConfig);
    return connection->CreateClient(clientOptions);
}

Y_UNIT_TEST_SUITE(CypressClient)
{
    Y_UNIT_TEST(ListCypressRoot)
    {
        TLogManager::Get()->ConfigureFromEnv();
        TAddressResolver::Get()->Configure(New<TAddressResolverConfig>());

        auto client = CreateTestRpcClient();

        client->GetNode("//@").Get().ValueOrThrow();
    }
}
