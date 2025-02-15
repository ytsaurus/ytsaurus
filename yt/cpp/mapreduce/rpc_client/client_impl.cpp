#include "raw_client.h"

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/client/init.h>

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateRpcClient(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    auto context = NDetail::CreateClientContext(serverName, options);

    auto globalTxId = GetGuid(context.Config->GlobalTxId);

    auto retryConfigProvider = options.RetryConfigProvider_;
    if (!retryConfigProvider) {
        retryConfigProvider = CreateDefaultRetryConfigProvider();
    }

    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    connectionConfig->ClusterUrl = context.ServerName;
    if (options.ProxyRole_) {
        connectionConfig->ProxyRole = *options.ProxyRole_;
    }
    if (context.ProxyAddress) {
        connectionConfig->ProxyAddresses = {*context.ProxyAddress};
    }

    NApi::TClientOptions clientOptions;
    clientOptions.Token = context.Token;
    if (context.ServiceTicketAuth) {
        clientOptions.ServiceTicketAuth = context.ServiceTicketAuth->Ptr;
    }
    if (context.ImpersonationUser) {
        clientOptions.User = *context.ImpersonationUser;
    }

    auto connection = NApi::NRpcProxy::CreateConnection(connectionConfig);

    auto rawClient = MakeIntrusive<NDetail::TRpcRawClient>(
        connection->CreateClient(clientOptions),
        context.Config);

    NDetail::EnsureInitialized();

    return new NDetail::TClient(
        std::move(rawClient),
        context,
        globalTxId,
        CreateDefaultClientRetryPolicy(retryConfigProvider, context.Config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
