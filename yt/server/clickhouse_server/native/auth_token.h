#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

struct IAuthorizationToken
{
    virtual ~IAuthorizationToken() = default;

    /// Opaque token used to represent authenticated client session
};

////////////////////////////////////////////////////////////////////////////////

struct IAuthorizationTokenService
{
    virtual ~IAuthorizationTokenService() = default;

    virtual IAuthorizationTokenPtr CreateToken(const THashMap<TString, TString>& attrs) = 0;
};

////////////////////////////////////////////////////////////////////////////////

const NApi::TClientOptions& UnwrapAuthToken(const IAuthorizationToken& token);

////////////////////////////////////////////////////////////////////////////////

IAuthorizationTokenService* GetAuthTokenService();

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
