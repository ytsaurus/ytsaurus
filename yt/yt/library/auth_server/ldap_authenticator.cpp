#include "ldap_authenticator.h"

#include "config.h"
#include "ldap_helpers.h"
#include "private.h"

#include <yt/yt/core/actions/bind.h>

#include <ldap.h>

namespace NYT::NAuth {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

berval MakeBerval(TStringBuf s)
{
    return berval{
        .bv_len = static_cast<ber_len_t>(s.size()),
        .bv_val = const_cast<char*>(s.data()),
    };
}

timeval MakeTimeval(TDuration duration)
{
    return timeval{
        .tv_sec = static_cast<long>(duration.Seconds()),
        .tv_usec = 0,
    };
}

[[noreturn]] void ThrowLdapError(int rc, TStringBuf operation, TStringBuf context)
{
    THROW_ERROR_EXCEPTION("LDAP %v failed%v%v: %v",
        operation,
        context.empty() ? TStringBuf() : TStringBuf(" for "),
        context,
        ldap_err2string(rc));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TLdapLoginAuthenticator
    : public ILoginAuthenticator
{
public:
    TLdapLoginAuthenticator(TLdapServiceConfigPtr config, IInvokerPtr invoker)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , Url_(Format(
            "%v://%v:%v",
            Config_->Encryption == ELdapEncryption::Ldaps ? "ldaps" : "ldap",
            Config_->Host,
            *Config_->Port))
        , AdminPassword_(Config_->GetAdminPassword())
    { }

    TFuture<TLoginResult> Authenticate(const TLoginCredentials& credentials) override
    {
        YT_LOG_DEBUG("Trying LDAP authentication (User: %v)", credentials.User);
        return BIND(&TLdapLoginAuthenticator::DoAuthenticate, MakeStrong(this), credentials)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const TLdapServiceConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TString Url_;
    const TString AdminPassword_;

    //! RAII wrapper that unbinds the connection on destruction.
    class TLdapConnection
    {
    public:
        explicit TLdapConnection(LDAP* ld)
            : Ld_(ld)
        { }

        ~TLdapConnection()
        {
            if (Ld_) {
                ldap_unbind_ext_s(Ld_, nullptr, nullptr);
            }
        }

        TLdapConnection(const TLdapConnection&) = delete;
        TLdapConnection& operator=(const TLdapConnection&) = delete;

        LDAP* Get() const
        {
            return Ld_;
        }

    private:
        LDAP* Ld_;
    };

    //! Establishes a connection, configures options and performs StartTLS
    //! (if required). Returns a handle with RAII cleanup.
    std::unique_ptr<TLdapConnection> Connect() const
    {
        LDAP* ld = nullptr;
        int rc = ldap_initialize(&ld, Url_.c_str());
        if (rc != LDAP_SUCCESS) {
            ThrowLdapError(rc, "initialize", Url_);
        }
        auto connection = std::make_unique<TLdapConnection>(ld);

        ConfigureOptions(ld);

        if (Config_->Encryption == ELdapEncryption::StartTls) {
            rc = ldap_start_tls_s(ld, nullptr, nullptr);
            if (rc != LDAP_SUCCESS) {
                ThrowLdapError(rc, "StartTLS", Url_);
            }
        }
        return connection;
    }

    void ConfigureOptions(LDAP* ld) const
    {
        int version = LDAP_VERSION3;
        ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version);

        auto timeout = MakeTimeval(Config_->RequestTimeout);
        ldap_set_option(ld, LDAP_OPT_TIMEOUT, &timeout);
        ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &timeout);

        // Require TLS certificate verification when TLS is used.
        // Without explicit CA file, OpenLDAP falls back to the system CA store,
        // so the default is still secure.
        if (Config_->Encryption != ELdapEncryption::None) {
            if (Config_->CertificateAuthority && Config_->CertificateAuthority->FileName) {
                ldap_set_option(ld, LDAP_OPT_X_TLS_CACERTFILE,
                    Config_->CertificateAuthority->FileName->c_str());
            }
            int tlsVerify = LDAP_OPT_X_TLS_DEMAND;
            ldap_set_option(ld, LDAP_OPT_X_TLS_REQUIRE_CERT, &tlsVerify);
        }
    }

    //! Performs a simple bind. Returns the raw LDAP result code so the caller
    //! can distinguish invalid credentials from other errors.
    int SimpleBind(LDAP* ld, TStringBuf dn, TStringBuf password) const
    {
        auto cred = MakeBerval(password);
        return ldap_sasl_bind_s(
            ld,
            TString(dn).c_str(),
            LDAP_SASL_SIMPLE,
            &cred,
            nullptr,
            nullptr,
            nullptr);
    }

    //! Resolves user DN via admin-bound search.
    TString ResolveUserDn(LDAP* ld, const TString& user) const
    {
        auto filter = BuildSearchFilter(Config_->SearchFilter, user);

        auto timeout = MakeTimeval(Config_->RequestTimeout);

        // We only need the DN, not attributes.
        char* noAttrs[] = {const_cast<char*>(LDAP_NO_ATTRS), nullptr};

        LDAPMessage* rawSearchResult = nullptr;
        int rc = ldap_search_ext_s(
            ld,
            Config_->SearchBase.c_str(),
            LDAP_SCOPE_SUBTREE,
            filter.c_str(),
            noAttrs,
            /*attrsonly*/ 0,
            nullptr,
            nullptr,
            &timeout,
            /*sizelimit*/ 2,  // 2 to detect ambiguous results.
            &rawSearchResult);
        auto searchGuard = Finally([rawSearchResult] {
            if (rawSearchResult) {
                ldap_msgfree(rawSearchResult);
            }
        });

        if (rc != LDAP_SUCCESS) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "LDAP search failed for user %Qv: %v",
                user,
                ldap_err2string(rc));
        }

        int entryCount = ldap_count_entries(ld, rawSearchResult);
        if (entryCount == 0) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "User %Qv not found in LDAP directory",
                user);
        }
        if (entryCount > 1) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Ambiguous LDAP search result for user %Qv: %v entries found",
                user,
                entryCount);
        }

        auto* entry = ldap_first_entry(ld, rawSearchResult);
        char* rawDn = ldap_get_dn(ld, entry);
        if (!rawDn) {
            int err = 0;
            ldap_get_option(ld, LDAP_OPT_RESULT_CODE, &err);
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "LDAP failed to retrieve DN for user %Qv: %v",
                user,
                ldap_err2string(err));
        }
        auto dnGuard = Finally([rawDn] { ldap_memfree(rawDn); });
        return TString(rawDn);
    }

    TLoginResult DoAuthenticate(const TLoginCredentials& credentials)
    {
        const auto& user = credentials.User;
        const auto& password = credentials.Password;

        // We reuse a single connection: admin bind -> search -> rebind as user.
        // This avoids a second TCP + TLS handshake.
        auto connection = Connect();
        auto* ld = connection->Get();

        if (int rc = SimpleBind(ld, Config_->AdminDn, AdminPassword_);
            rc != LDAP_SUCCESS)
        {
            THROW_ERROR_EXCEPTION("LDAP admin bind failed for %v as %Qv: %v",
                Url_,
                Config_->AdminDn,
                ldap_err2string(rc));
        }

        auto userDn = ResolveUserDn(ld, user);
        YT_LOG_DEBUG("Found user DN in LDAP (User: %v, UserDn: %v)", user, userDn);

        if (int rc = SimpleBind(ld, userDn, password);
            rc != LDAP_SUCCESS)
        {
            // Map all user-bind failures (including LDAP_INVALID_CREDENTIALS)
            // to InvalidCredentials — this is user-facing input.
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Invalid LDAP credentials for user %Qv: %v",
                user,
                ldap_err2string(rc));
        }

        YT_LOG_DEBUG("LDAP authentication succeeded (User: %v)", user);
        return TLoginResult{.Login = user, .Source = EAuthSource::Ldap};
    }
};

////////////////////////////////////////////////////////////////////////////////

ILoginAuthenticatorPtr CreateLdapLoginAuthenticator(
    TLdapServiceConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TLdapLoginAuthenticator>(std::move(config), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
