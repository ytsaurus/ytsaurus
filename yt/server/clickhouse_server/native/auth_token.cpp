#include "auth_token.h"

#include <yt/client/api/connection.h>

#include <yt/core/misc/optional.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<TString> GetValue(const THashMap<TString, TString>& attrs, TStringBuf name)
{
    auto it = attrs.find(name);
    if (it != attrs.end()) {
        return it->second;
    }
    return {};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TClientAuthToken
    : public IAuthorizationToken
    , public TClientOptions
{
public:
    TClientAuthToken() = default;

    TClientAuthToken(const TClientOptions& options)
        : TClientOptions(options)
    {}
};

////////////////////////////////////////////////////////////////////////////////

const TClientOptions& UnwrapAuthToken(const IAuthorizationToken& token)
{
    const auto* ptr = dynamic_cast<const TClientOptions*>(&token);
    if (!ptr) {
        THROW_ERROR_EXCEPTION("Invalid authorization token");
    }
    return *ptr;
}

////////////////////////////////////////////////////////////////////////////////

class TAuthTokenService
    : public IAuthorizationTokenService
{
public:
    IAuthorizationTokenPtr CreateToken(const THashMap<TString, TString>& attrs) override
    {
        auto user = GetValue(attrs, "user");
        if (!user) {
            THROW_ERROR_EXCEPTION("Invalid client credentials: expected user login");
        }

        TClientOptions options;
        options.PinnedUser = *user;
        options.Token = GetValue(attrs, "token");
        options.SessionId = GetValue(attrs, "sessionId");
        options.SslSessionId = GetValue(attrs, "sessionId2");

        return std::make_shared<TClientAuthToken>(options);
    }
};

////////////////////////////////////////////////////////////////////////////////

IAuthorizationTokenService* GetAuthTokenService()
{
    static TAuthTokenService instance;
    return &instance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
