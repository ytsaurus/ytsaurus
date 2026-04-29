#include "cypress_login_authenticator.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

static const std::string HashedPasswordAttribute = "hashed_password";
static const std::string PasswordSaltAttribute = "password_salt";

////////////////////////////////////////////////////////////////////////////////

class TCypressLoginAuthenticator
    : public ILoginAuthenticator
{
public:
    explicit TCypressLoginAuthenticator(IClientPtr client)
        : Client_(std::move(client))
    { }

    TFuture<TLoginResult> Authenticate(const TLoginCredentials& credentials) override
    {
        const auto& user = credentials.User;
        const auto& password = credentials.Password;

        YT_LOG_DEBUG("Trying Cypress password authentication (User: %v)", user);

        auto path = Format("//sys/users/%v", ToYPathLiteral(user));

        TGetNodeOptions options;
        options.Attributes = {HashedPasswordAttribute, PasswordSaltAttribute};

        return Client_->GetNode(path, options)
            .Apply(BIND([user, password] (const TYsonString& rsp) {
                auto node = ConvertToNode(rsp);
                auto hashedPassword = node->Attributes().Get<TString>(HashedPasswordAttribute);
                auto passwordSalt = node->Attributes().Get<TString>(PasswordSaltAttribute);

                if (HashPassword(password, passwordSalt) != hashedPassword) {
                    THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                        "Invalid password for user %Qlv",
                        user);
                }
                return TLoginResult{.Login = user, .Source = EAuthSource::Cypress};
            }));
    }

private:
    const IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

ILoginAuthenticatorPtr CreateCypressLoginAuthenticator(IClientPtr client)
{
    return New<TCypressLoginAuthenticator>(std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
