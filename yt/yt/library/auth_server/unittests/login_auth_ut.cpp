#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/ldap_helpers.h>
#include <yt/yt/library/auth_server/login_authenticator.h>

#include <util/system/env.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

class TFixedLoginAuthenticator
    : public ILoginAuthenticator
{
public:
    struct TCall
    {
        TString User;
        TString Password;
    };

    explicit TFixedLoginAuthenticator(TErrorOr<TLoginResult> result)
        : Result_(std::move(result))
    { }

    TFuture<TLoginResult> Authenticate(const TLoginCredentials& credentials) override
    {
        Calls_.push_back({credentials.User, credentials.Password});
        return Result_.IsOK()
            ? MakeFuture(Result_.Value())
            : MakeFuture<TLoginResult>(Result_);
    }

    const std::vector<TCall>& Calls() const { return Calls_; }
    int CallCount() const { return static_cast<int>(Calls_.size()); }

private:
    TErrorOr<TLoginResult> Result_;
    std::vector<TCall> Calls_;
};

using TFixedLoginAuthenticatorPtr = TIntrusivePtr<TFixedLoginAuthenticator>;

TFixedLoginAuthenticatorPtr MakeFixed(TErrorOr<TLoginResult> result)
{
    return New<TFixedLoginAuthenticator>(std::move(result));
}

TFixedLoginAuthenticatorPtr MakeFixedOk(TString login, EAuthSource source = EAuthSource::Cypress)
{
    return MakeFixed(TLoginResult{.Login = std::move(login), .Source = source});
}

TFixedLoginAuthenticatorPtr MakeFixedErr(TError error)
{
    return MakeFixed(std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeLoginAuthenticatorTest, FirstSucceeds)
{
    auto first = MakeFixedOk("alice");
    auto second = MakeFixedErr(TError(NRpc::EErrorCode::InvalidCredentials, "bad"));

    auto composite = CreateCompositeLoginAuthenticator({first, second});
    auto result = WaitFor(composite->Authenticate(TLoginCredentials{"alice", "pass"}));

    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ(result.Value().Login, "alice");
    EXPECT_EQ(result.Value().Source, EAuthSource::Cypress);
    // Second must not be called when first succeeds.
    EXPECT_EQ(second->CallCount(), 0);
}

TEST(TCompositeLoginAuthenticatorTest, FallsBackToSecond)
{
    auto first = MakeFixedErr(TError(NRpc::EErrorCode::InvalidCredentials, "first failed"));
    auto second = MakeFixedOk("bob", EAuthSource::Ldap);

    auto composite = CreateCompositeLoginAuthenticator({first, second});
    auto result = WaitFor(composite->Authenticate(TLoginCredentials{"bob", "pass"}));

    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ(result.Value().Login, "bob");
    EXPECT_EQ(result.Value().Source, EAuthSource::Ldap);
    EXPECT_EQ(first->CallCount(), 1);
    EXPECT_EQ(second->CallCount(), 1);
}

TEST(TCompositeLoginAuthenticatorTest, CredentialsPassedThrough)
{
    auto auth = MakeFixedOk("carol");
    auto composite = CreateCompositeLoginAuthenticator({auth});

    WaitFor(composite->Authenticate(TLoginCredentials{"carol", "s3cr3t"})).ThrowOnError();

    ASSERT_EQ(auth->CallCount(), 1);
    EXPECT_EQ(auth->Calls()[0].User, "carol");
    EXPECT_EQ(auth->Calls()[0].Password, "s3cr3t");
}

TEST(TCompositeLoginAuthenticatorTest, AllFailReturnsCombinedError)
{
    auto composite = CreateCompositeLoginAuthenticator({
        MakeFixedErr(TError(NRpc::EErrorCode::InvalidCredentials, "first failed")),
        MakeFixedErr(TError(NRpc::EErrorCode::InvalidCredentials, "second failed")),
    });

    auto result = WaitFor(composite->Authenticate(TLoginCredentials{"x", "y"}));

    ASSERT_FALSE(result.IsOK());
    EXPECT_TRUE(result.FindMatching(NRpc::EErrorCode::InvalidCredentials).has_value());
    // Both inner errors must be present.
    auto msg = ToString(result);
    EXPECT_THAT(msg, ::testing::HasSubstr("first failed"));
    EXPECT_THAT(msg, ::testing::HasSubstr("second failed"));
}

TEST(TCompositeLoginAuthenticatorTest, EmptyAlwaysFails)
{
    auto composite = CreateCompositeLoginAuthenticator({});

    auto result = WaitFor(composite->Authenticate(TLoginCredentials{"x", "y"}));
    ASSERT_FALSE(result.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

// Loads a TLdapServiceConfig from a YSON map node. Throws on validation error.
TLdapServiceConfigPtr LoadLdapConfig(NYTree::IMapNodePtr node)
{
    auto config = New<TLdapServiceConfig>();
    config->Load(node);
    return config;
}

// Base LDAP node without any password source — callers must supply one via fn.
NYTree::IMapNodePtr MakeLdapNode(std::function<void(NYTree::TFluentMap)> fn)
{
    return NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("host").Value("ldap.example.com")
            .Item("admin_dn").Value("cn=admin,dc=example,dc=com")
            .Item("search_base").Value("dc=example,dc=com")
            .Do(fn)
        .EndMap()
        ->AsMap();
}

// Convenience wrapper that sets the admin password via env var.
NYTree::IMapNodePtr MakeLdapNodeWithPassword(std::function<void(NYTree::TFluentMap)> fn = [] (auto) {})
{
    SetEnv("YT_TEST_LDAP_ADMIN_PASSWORD", "secret");
    return MakeLdapNode([fn] (auto map) {
        map.Item("admin_password_env_var").Value("YT_TEST_LDAP_ADMIN_PASSWORD");
        fn(map);
    });
}

TEST(TLdapServiceConfigTest, DefaultPortNone)
{
    auto config = LoadLdapConfig(MakeLdapNodeWithPassword());
    EXPECT_EQ(*config->Port, 389);
    EXPECT_EQ(config->Encryption, ELdapEncryption::None);
}

TEST(TLdapServiceConfigTest, DefaultPortLdaps)
{
    auto config = LoadLdapConfig(MakeLdapNodeWithPassword([] (auto map) {
        map.Item("encryption").Value("ldaps");
    }));
    EXPECT_EQ(*config->Port, 636);
    EXPECT_EQ(config->Encryption, ELdapEncryption::Ldaps);
}

TEST(TLdapServiceConfigTest, DefaultPortStartTls)
{
    auto config = LoadLdapConfig(MakeLdapNodeWithPassword([] (auto map) {
        map.Item("encryption").Value("start_tls");
    }));
    EXPECT_EQ(*config->Port, 389);
    EXPECT_EQ(config->Encryption, ELdapEncryption::StartTls);
}

TEST(TLdapServiceConfigTest, ExplicitPortPreserved)
{
    auto config = LoadLdapConfig(MakeLdapNodeWithPassword([] (auto map) {
        map.Item("encryption").Value("ldaps");
        map.Item("port").Value(1636);
    }));
    EXPECT_EQ(*config->Port, 1636);
}

TEST(TLdapServiceConfigTest, GetAdminPasswordFromEnvVar)
{
    SetEnv("YT_TEST_LDAP_PASSWORD", "env_secret");
    auto config = LoadLdapConfig(MakeLdapNode([] (auto map) {
        map.Item("admin_password_env_var").Value("YT_TEST_LDAP_PASSWORD");
    }));
    EXPECT_EQ(config->GetAdminPassword(), "env_secret");
    UnsetEnv("YT_TEST_LDAP_PASSWORD");
}

TEST(TLdapServiceConfigTest, GetAdminPasswordEnvVarMissingThrows)
{
    UnsetEnv("YT_TEST_LDAP_PASSWORD_MISSING");
    auto config = LoadLdapConfig(MakeLdapNode([] (auto map) {
        map.Item("admin_password_env_var").Value("YT_TEST_LDAP_PASSWORD_MISSING");
    }));
    EXPECT_THROW_WITH_SUBSTRING(config->GetAdminPassword(), "is not set");
}

TEST(TLdapServiceConfigTest, NoPasswordSourceThrows)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadLdapConfig(MakeLdapNode([] (auto) {})),
        "Exactly one");
}

TEST(TLdapServiceConfigTest, MultiplePasswordSourcesThrows)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadLdapConfig(MakeLdapNode([] (auto map) {
            map.Item("admin_password_path").Value("/tmp/ldap_pass");
            map.Item("admin_password_env_var").Value("YT_FOO");
        })),
        "Exactly one");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLdapFilterTest, NoEscapingNeeded)
{
    EXPECT_EQ(LdapEscapeFilterValue("alice"), "alice");
    EXPECT_EQ(LdapEscapeFilterValue("user123"), "user123");
    EXPECT_EQ(LdapEscapeFilterValue(""), "");
}

TEST(TLdapFilterTest, EscapesSpecialChars)
{
    EXPECT_EQ(LdapEscapeFilterValue("*"), "\\2a");
    EXPECT_EQ(LdapEscapeFilterValue("("), "\\28");
    EXPECT_EQ(LdapEscapeFilterValue(")"), "\\29");
    EXPECT_EQ(LdapEscapeFilterValue("\\"), "\\5c");
    EXPECT_EQ(LdapEscapeFilterValue(TStringBuf("\0", 1)), "\\00");
}

TEST(TLdapFilterTest, EscapesMixed)
{
    EXPECT_EQ(LdapEscapeFilterValue("foo*bar(baz)"), "foo\\2abar\\28baz\\29");
}

TEST(TLdapFilterTest, EscapesInjectionAttempt)
{
    EXPECT_EQ(
        LdapEscapeFilterValue("*)(uid=*))(|(uid=*"),
        "\\2a\\29\\28uid=\\2a\\29\\29\\28|\\28uid=\\2a");
}

TEST(TLdapFilterTest, BuildSearchFilterSubstitutes)
{
    EXPECT_EQ(BuildSearchFilter("(uid={login})", "alice"), "(uid=alice)");
}

TEST(TLdapFilterTest, BuildSearchFilterEscapes)
{
    EXPECT_EQ(BuildSearchFilter("(uid={login})", "al*ce"), "(uid=al\\2ace)");
}

TEST(TLdapFilterTest, BuildSearchFilterMultiplePlaceholders)
{
    EXPECT_EQ(
        BuildSearchFilter("(|(uid={login})(mail={login}@example.com))", "bob"),
        "(|(uid=bob)(mail=bob@example.com))");
}

TEST(TLdapFilterTest, BuildSearchFilterNoPlaceholder)
{
    EXPECT_EQ(BuildSearchFilter("(objectClass=person)", "alice"), "(objectClass=person)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
