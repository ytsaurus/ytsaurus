#include "cypress_cookie.h"

#include "config.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NAuth {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

TString TCypressCookie::ToHeader(const TCypressCookieGeneratorConfigPtr& config) const
{
    auto header = Format("%v=%v; Expires=%v",
        CypressCookieName,
        Value,
        ExpiresAt.ToRfc822String());
    if (config->Secure) {
        header += "; Secure";
    }
    if (const auto& domain = config->Domain) {
        header += Format("; Domain=%v", domain);
    }
    if (const auto& path = config->Path) {
        header += Format("; Path=%v", path);
    }

    return header;
}

void TCypressCookie::Register(TRegistrar registrar)
{
    registrar.Parameter("value", &TThis::Value);
    registrar.Parameter("user", &TThis::User);
    registrar.Parameter("password_revision", &TThis::PasswordRevision);
    registrar.Parameter("expires_at", &TThis::ExpiresAt);
}

////////////////////////////////////////////////////////////////////////////////

TString GenerateCookieValue()
{
    constexpr int ValueSize = 32;

    auto rawCookie = GenerateCryptoStrongRandomString(ValueSize);
    return HexEncode(rawCookie.data(), rawCookie.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
