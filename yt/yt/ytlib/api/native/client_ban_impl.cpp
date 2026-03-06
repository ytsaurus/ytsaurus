#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "helpers.h"
#include "private.h"
#include "transaction.h"

#include <yt/yt/ytlib/ban_client/ban_service_proxy.h>

#include <yt/yt/client/api/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NBanClient;
using namespace NSecurityClient;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

NRpc::IChannelPtr GetCypressProxyChannelOrThrow(const NApi::NNative::IConnectionPtr& connection)
{
    auto cypressProxyChannel = connection->GetCypressProxyChannel();
    if (!cypressProxyChannel) {
        THROW_ERROR_EXCEPTION("Method is not supported for cluster without cypress proxy");
    }
    return cypressProxyChannel;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TClient::DoSetUserBanned(const std::string& user, bool isBanned, const TSetUserBannedOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::SetUserBanned,
        EPermission::Use);

    auto channel = WrapChannel(GetCypressProxyChannelOrThrow(Connection_));

    TBanServiceProxy proxy(channel);
    auto req = proxy.SetUserBanned();
    req->SetTimeout(options.Timeout.value_or(NRpc::HugeDoNotUseRpcRequestTimeout));
    req->set_user_name(user);
    req->set_is_banned(isBanned);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

bool TClient::DoGetUserBanned(const std::string& user, const TGetUserBannedOptions& options)
{
    auto channel = WrapChannel(GetCypressProxyChannelOrThrow(Connection_));

    TBanServiceProxy proxy(channel);
    auto req = proxy.GetUserBanned();
    req->SetTimeout(options.Timeout.value_or(NRpc::HugeDoNotUseRpcRequestTimeout));
    req->set_user_name(user);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return rsp->is_banned();
}

std::vector<std::string> TClient::DoListBannedUsers(const TListBannedUsersOptions& options)
{
    auto channel = WrapChannel(GetCypressProxyChannelOrThrow(Connection_));

    TBanServiceProxy proxy(channel);
    auto req = proxy.ListBannedUsers();
    req->SetTimeout(options.Timeout.value_or(NRpc::HugeDoNotUseRpcRequestTimeout));

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    std::vector<std::string> bannedUsers;
    bannedUsers.reserve(rsp->user_names_size());
    for (const auto& bannedUser : rsp->user_names()) {
        bannedUsers.emplace_back(bannedUser);
    }

    return bannedUsers;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
