#include <library/unittest/registar.h>
#include <util/system/env.h>

#include <yt/client/api/client.h>

#include <yt/client/api/rpc_proxy/config.h>
#include <yt/client/api/rpc_proxy/connection.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/net/address.h>

using namespace NYT;
using namespace NYT::NApi;
using namespace NYT::NLogging;
using namespace NYT::NNet;

IClientPtr CreateTestRpcClient()
{
    auto proxyAddress = GetEnv("YT_RPC_PROXY");
    if (!proxyAddress) {
        THROW_ERROR_EXCEPTION("YT_RPC_PROXY environment variable is not set");
    }

    auto connectionConfig = New<NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    connectionConfig->Addresses.push_back(proxyAddress);

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
