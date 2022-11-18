#include "helpers.h"

#include "connection.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/auth/native_authenticator.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
