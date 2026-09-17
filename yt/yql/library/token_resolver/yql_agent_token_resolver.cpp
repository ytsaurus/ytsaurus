#include "yql_agent_token_resolver.h"

#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/ytlib/yql_client/token_service_proxy.h>

#include <util/datetime/base.h>

namespace NYql {

namespace {

NYT::NBus::IBusClientPtr CreateBusClient(const TYqlAgentTokenResolverConfig& config) {
    YQL_ENSURE(!config.GetUnixSocketPath().empty());
    auto busConfig = NYT::NBus::NTcp::TBusClientConfig::CreateUds(
        config.GetUnixSocketPath());
    return NYT::NBus::NTcp::CreateBusClient(busConfig);
}

class TYqlAgentTokenResolver : public IYtTokenResolver {
public:
    explicit TYqlAgentTokenResolver(const TYqlAgentTokenResolverConfig& config)
        : RpcTimeout_(TDuration::MilliSeconds(config.GetRpcTimeoutMs()))
        , BusClient_(CreateBusClient(config))
        , Channel_(NYT::NRpc::NBus::CreateBusChannel(BusClient_))
    { }

    TMaybe<TString> ResolveClusterToken(
        const TString& cluster,
        const TCredentials& credentials) override
    {
        const auto& queryIdentityToken = credentials.GetUserCredentials().QueryIdentityToken;
        if (queryIdentityToken.empty()) {
            return {};
        }

        NYT::NYqlClient::TTokenServiceProxy proxy(Channel_);
        proxy.SetDefaultTimeout(RpcTimeout_);
        auto request = proxy.IssueQueryTemporaryToken();
        request->set_query_identity_token(queryIdentityToken);
        request->set_cluster(cluster);

        YQL_CLOG(INFO, ProviderYt) << "Requesting temporary YT token from YQL agent for cluster " << cluster;

        try {
            auto response = request->Invoke().BlockingGet()
                .ValueOrThrow();

            YQL_CLOG(INFO, ProviderYt) << "Received temporary YT token from YQL agent for cluster " << cluster;
            return TString(response->token());
        } catch (const std::exception&) {
            YQL_CLOG(ERROR, ProviderYt) << "Failed to request temporary YT token from YQL agent for cluster "
                << cluster << ": " << CurrentExceptionMessage();
            throw;
        }
    }

private:
    const TDuration RpcTimeout_;
    const NYT::NBus::IBusClientPtr BusClient_;
    const NYT::NRpc::IChannelPtr Channel_;
};

} // namespace

IYtTokenResolver::TPtr CreateYqlAgentTokenResolver(const TYqlAgentTokenResolverConfig& config) {
    return MakeIntrusive<TYqlAgentTokenResolver>(config);
}

} // namespace NYql
