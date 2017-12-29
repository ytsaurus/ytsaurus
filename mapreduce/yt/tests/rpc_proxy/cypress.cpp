#include <library/unittest/registar.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/rpc_proxy/rpc_proxy_connection.h>
#include <yt/ytlib/rpc_proxy/config.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/address.h>

using namespace NYT;
using namespace NYT::NApi;
using namespace NYT::NLogging;
using namespace NYT::NRpcProxy;

IClientPtr CreateTestRpcClient()
{
    auto proxyAddress = getenv("YT_RPC_PROXY");
    if (!proxyAddress) {
        THROW_ERROR_EXCEPTION("YT_RPX_PROXY environment variable is not set");
    }

    auto connectionConfig = New<TRpcProxyConnectionConfig>();
    connectionConfig->SetDefaults();
    connectionConfig->Addresses.push_back(proxyAddress);

    TClientOptions clientOptions;
    auto connection = CreateRpcProxyConnection(connectionConfig);
    return connection->CreateClient(clientOptions);
}

SIMPLE_UNIT_TEST_SUITE(CypressClient)
{
    SIMPLE_UNIT_TEST(ListCypressRoot)
    {
        TLogManager::Get()->ConfigureFromEnv();
        TAddressResolver::Get()->Configure(New<TAddressResolverConfig>());

        auto client = CreateTestRpcClient();

        client->GetNode("//@").Get().ValueOrThrow();
    }
}
