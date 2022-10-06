#include "helpers.h"

#include "connection.h"
#include "config.h"

#include <yt/yt/ytlib/auth/native_authenticator.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

namespace NYT::NApi::NNative {

using namespace NAuth;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

bool IsValidSourceTvmId(const IConnectionPtr& connection, TTvmId tvmId)
{
    return tvmId == connection->GetConfig()->TvmId || connection->GetClusterDirectory()->HasTvmId(tvmId);
}

IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection)
{
    return NAuth::CreateNativeAuthenticator([connection] (TTvmId tvmId) {
        return IsValidSourceTvmId(connection, tvmId);
    });
}

IChannelFactoryPtr CreateNativeAuthenticationInjectingChannelFactory(
    IChannelFactoryPtr channelFactory,
    std::optional<TTvmId> tvmId)
{
    auto tvmService = TNativeAuthenticationManager::Get()->GetTvmService();
    if (tvmId && !tvmService) {
        THROW_ERROR_EXCEPTION("Cluster connection requires TVM authentification, but TVM service is unset");
    }
    if (tvmService && tvmId) {
        auto ticketAuth = CreateServiceTicketAuth(tvmService, *tvmId);
        channelFactory = CreateServiceTicketInjectingChannelFactory(
            std::move(channelFactory),
            std::move(ticketAuth));
    }
    return channelFactory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
