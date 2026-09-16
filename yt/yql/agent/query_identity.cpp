#include "query_identity.h"

#include <yt/yt/ytlib/yql_client/proto/token_service.pb.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NYqlAgent {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int QueryIdentitySecretLength = 32;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TQueryIdentityAuthority::TQueryIdentityAuthority()
    : Secret_(GenerateCryptoStrongRandomString(QueryIdentitySecretLength))
{ }

TString TQueryIdentityAuthority::IssueToken(TExecutionId executionId) const
{
    NYqlClient::NProto::TQueryIdentityToken token;
    ToProto(token.mutable_execution_id(), executionId);
    token.set_signature(CreateSha256HmacRaw(
        Secret_,
        token.execution_id().SerializeAsString()));
    return token.SerializeAsString();
}

TExecutionId TQueryIdentityAuthority::ValidateToken(const TString& serializedToken) const
{
    NYqlClient::NProto::TQueryIdentityToken token;
    if (!token.ParsePartialFromString(serializedToken) || !token.IsInitialized()) {
        THROW_ERROR_EXCEPTION("Malformed query identity token");
    }

    auto expectedSignature = CreateSha256HmacRaw(
        Secret_,
        token.execution_id().SerializeAsString());
    if (!ConstantTimeCompare(expectedSignature, token.signature())) {
        THROW_ERROR_EXCEPTION("Invalid query identity token signature");
    }

    return FromProto<TExecutionId>(token.execution_id());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
