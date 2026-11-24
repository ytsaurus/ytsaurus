#include <yt/yt/flow/extensions/ydb/cpp/credentials.h>
#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const std::string TokenSentinel = "token-sentinel-value";
const std::string PasswordSentinel = "password-sentinel-value";

class TRecordingEnvironment
{
public:
    explicit TRecordingEnvironment(THashMap<std::string, std::string> values)
        : Values_(std::move(values))
    { }

    TYdbEnvironmentReader GetReader()
    {
        return [this] (const std::string& name) {
            RequestedNames_.push_back(name);
            auto it = Values_.find(name);
            return it == Values_.end() ? std::string() : it->second;
        };
    }

    const std::vector<std::string>& GetRequestedNames() const
    {
        return RequestedNames_;
    }

private:
    const THashMap<std::string, std::string> Values_;
    std::vector<std::string> RequestedNames_;
};

std::string GetAuthInfo(const std::shared_ptr<NYdb::ICredentialsProviderFactory>& factory)
{
    return factory->CreateProvider()->GetAuthInfo();
}

TEST(TYdbCredentialsTest, AutoUsesDefaultTokenVariable)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", TokenSentinel}});
    auto factory = ResolveYdbCredentials({}, environment.GetReader(), nullptr);
    ASSERT_TRUE(factory);
    EXPECT_EQ(GetAuthInfo(factory), TokenSentinel);
    EXPECT_EQ(environment.GetRequestedNames(), std::vector<std::string>{"YDB_TOKEN"});
}

TEST(TYdbCredentialsTest, AutoUsesCustomTokenVariable)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", "ambient"}, {"CUSTOM_TOKEN", TokenSentinel}});
    auto factory = ResolveYdbCredentials({.TokenEnvVar = "CUSTOM_TOKEN"}, environment.GetReader(), nullptr);
    ASSERT_TRUE(factory);
    EXPECT_EQ(GetAuthInfo(factory), TokenSentinel);
    EXPECT_EQ(environment.GetRequestedNames(), std::vector<std::string>{"CUSTOM_TOKEN"});
}

TEST(TYdbCredentialsTest, AutoWithoutTokenIsAnonymous)
{
    TRecordingEnvironment environment({});
    EXPECT_FALSE(ResolveYdbCredentials({}, environment.GetReader(), nullptr));
}

TEST(TYdbCredentialsTest, TokenModeRequiresToken)
{
    TRecordingEnvironment environment({});
    EXPECT_THROW_WITH_SUBSTRING(
        ResolveYdbCredentials({.AuthMode = EYdbAuthMode::Token}, environment.GetReader(), nullptr),
        "Configured token environment variable \"YDB_TOKEN\" is empty");
}

TEST(TYdbCredentialsTest, LoginReadsOnlyPasswordVariable)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", TokenSentinel}, {"YDB_PASSWORD", PasswordSentinel}});
    auto factory = ResolveYdbCredentials(
        {.AuthMode = EYdbAuthMode::Login, .Username = "user", .PasswordEnvVar = "YDB_PASSWORD"},
        environment.GetReader(),
        nullptr);
    EXPECT_TRUE(factory);
    EXPECT_EQ(environment.GetRequestedNames(), std::vector<std::string>{"YDB_PASSWORD"});
}

TEST(TYdbCredentialsTest, LoginRequiresPassword)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", TokenSentinel}});
    EXPECT_THROW_WITH_SUBSTRING(
        ResolveYdbCredentials(
            {.AuthMode = EYdbAuthMode::Login, .Username = "user", .PasswordEnvVar = "YDB_PASSWORD"},
            environment.GetReader(),
            nullptr),
        "Configured password environment variable \"YDB_PASSWORD\" is empty");
}

TEST(TYdbCredentialsTest, AnonymousIgnoresAmbientToken)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", TokenSentinel}});
    EXPECT_FALSE(ResolveYdbCredentials({.AuthMode = EYdbAuthMode::Anonymous}, environment.GetReader(), nullptr));
    EXPECT_TRUE(environment.GetRequestedNames().empty());
}

TEST(TYdbCredentialsTest, InjectedProviderIsPreservedWithoutEnvironmentAccess)
{
    TRecordingEnvironment environment({{"YDB_TOKEN", TokenSentinel}});
    auto injected = NYdb::CreateOAuthCredentialsProviderFactory("injected");
    EXPECT_EQ(ResolveYdbCredentials({}, environment.GetReader(), injected), injected);
    EXPECT_TRUE(environment.GetRequestedNames().empty());
}

TEST(TYdbCredentialsTest, DriverConfigHoldsOnlyVariableNames)
{
    // clang-format off
    auto config = NYTree::ConvertTo<TYdbTopicDriverConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("auth_mode").Value("login")
            .Item("username").Value("user")
            .Item("password_env_var").Value("YDB_PASSWORD")
        .EndMap());
    // clang-format on
    auto serialized = NYson::ConvertToYsonString(config, NYson::EYsonFormat::Text).ToString();
    EXPECT_EQ(serialized.find(PasswordSentinel), std::string::npos);
    EXPECT_EQ(serialized.find(TokenSentinel), std::string::npos);
    EXPECT_NE(serialized.find("YDB_PASSWORD"), std::string::npos);
    EXPECT_EQ(config->GetCredentialsConfig().PasswordEnvVar, "YDB_PASSWORD");
}

TEST(TYdbCredentialsTest, DriverConfigValidatesLoginNames)
{
    // clang-format off
    EXPECT_THROW(
        NYTree::ConvertTo<TYdbTopicDriverConfigPtr>(NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("auth_mode").Value("login")
                .Item("username").Value("user")
            .EndMap()),
        std::exception);
    EXPECT_THROW(
        NYTree::ConvertTo<TYdbTopicDriverConfigPtr>(NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("auth_mode").Value("token")
                .Item("token_env_var").Value("")
            .EndMap()),
        std::exception);
    // clang-format on
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
