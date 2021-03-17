#include "mock_http_server.h"

#include <yt/yt/ytlib/auth/config.h>
#include <yt/yt/ytlib/auth/default_secret_vault_service.h>
#include <yt/yt/ytlib/auth/secret_vault_service.h>
#include <yt/yt/ytlib/auth/tvm_service.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NTests;

using ::testing::HasSubstr;

////////////////////////////////////////////////////////////////////////////////

static const TString SecretId = "secret-id";
static const TString SecretVersion = "secret-version";
static const TString SecretDelegationToken = "secret-token";
static const TString SecretSignature = "secret-signature";
static const TString SecretKey = "secret-key";
static const TString SecretValue = "secret-value";
static const TString CyrillicValue = "секретное-значение";
static const TString HighAsciiValue = "secret-value-Æ";
static const TString SecretEncoding = "EBCDIC";
static const TString UserTicket = "the-user-ticket";

class TDefaultSecretVaultTest
    : public ::testing::Test
{
protected:
    TDefaultSecretVaultServiceConfigPtr CreateDefaultSecretVaultServiceConfig()
    {
        auto config = New<TDefaultSecretVaultServiceConfig>();
        config->Host = MockHttpServer_.GetHost();
        config->Port = MockHttpServer_.GetPort();
        config->Secure = false;
        config->RequestTimeout = TDuration::Seconds(1);
        config->Consumer = "yp.unittest";
        return config;
    }

    ISecretVaultServicePtr CreateDefaultSecretVaultService(
        TDefaultSecretVaultServiceConfigPtr config = {})
    {
        return NAuth::CreateDefaultSecretVaultService(
            config ? config : CreateDefaultSecretVaultServiceConfig(),
            New<TMockTvmService>(),
            CreateThreadPoolPoller(1, "HttpPoller"));
    }

    virtual void SetUp() override
    {
        MockHttpServer_.Start();
    }

    virtual void TearDown() override
    {
        if (MockHttpServer_.IsStarted()) {
            MockHttpServer_.Stop();
        }
    }

    void SetCallback(const TString& response)
    {
        MockHttpServer_.SetCallback([&] (TClientRequest* request) {
            const auto& firstLine = request->Input().FirstLine();
            const auto& body = request->Input().ReadAll();
            if (firstLine.StartsWith("POST /1/tokens/?consumer=yp.unittest")) {
                EXPECT_THAT(body, HasSubstr(SecretSignature));
                request->Output() << HttpResponse(200, response);
            } else if (firstLine.StartsWith("POST /1/secrets/secret-id/tokens/")) {
                EXPECT_THAT(body, HasSubstr(SecretSignature));
                request->Output() << HttpResponse(200, response);
            } else {
                request->Output() << HttpResponse(404, "");
            }
        });
    }

private:
    class TMockTvmService
        : public ITvmService
    {
    public:
        virtual ui32 GetSelfTvmId() override
        {
            return 100500;
        }

        virtual TString GetServiceTicket(const TString& serviceId) override
        {
            return Format("ticket:%v", serviceId);
        }

        virtual TParsedTicket ParseUserTicket(const TString& ticket) override
        {
            THROW_ERROR_EXCEPTION("Not implemented");
        }
    };

    TMockHttpServer MockHttpServer_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDefaultSecretVaultTest, WarningResponseStatus)
{
    TStringStream outputStream;
    auto consumer = NJson::CreateJsonConsumer(&outputStream);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("secrets")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("status").Value("warning")
                    .Item("warning_message").Value("version is hidden")
                    .Item("value")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("key").Value(SecretKey)
                            .Item("value").Value(SecretValue)
                            .Item("encoding").Value(SecretEncoding)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("status").Value("ok")
        .EndMap();

    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    std::vector<ISecretVaultService::TSecretSubrequest> subrequests;
    subrequests.push_back({
        SecretId,
        SecretVersion,
        SecretDelegationToken,
        SecretSignature});

    auto subresponses = WaitFor(service->GetSecrets(subrequests))
        .ValueOrThrow();
    ASSERT_EQ(1, subresponses.size());

    const auto& secrets = subresponses[0].ValueOrThrow().Values;
    ASSERT_EQ(1, secrets.size());
    ASSERT_EQ(SecretKey, secrets[0].Key);
    ASSERT_EQ(SecretValue, secrets[0].Value);
    ASSERT_EQ(SecretEncoding, secrets[0].Encoding);
}

TEST_F(TDefaultSecretVaultTest, NoEncoding)
{
    TStringStream outputStream;
    auto consumer = NJson::CreateJsonConsumer(&outputStream);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("secrets")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("status").Value("ok")
                    .Item("value")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("key").Value(SecretKey)
                            .Item("value").Value(SecretValue)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("status").Value("ok")
        .EndMap();

    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    std::vector<ISecretVaultService::TSecretSubrequest> subrequests;
    subrequests.push_back({
        SecretId,
        SecretVersion,
        SecretDelegationToken,
        SecretSignature});

    auto subresponses = WaitFor(service->GetSecrets(subrequests))
        .ValueOrThrow();
    ASSERT_EQ(1, subresponses.size());

    const auto& secrets = subresponses[0].ValueOrThrow().Values;
    ASSERT_EQ(1, secrets.size());
    ASSERT_EQ(SecretKey, secrets[0].Key);
    ASSERT_EQ(SecretValue, secrets[0].Value);
    ASSERT_TRUE(secrets[0].Encoding.empty());
}

TEST_F(TDefaultSecretVaultTest, Utf8)
{
    TStringStream outputStream;
    auto jsonConfig = New<NJson::TJsonFormatConfig>();
    jsonConfig->EncodeUtf8 = false; // Otherwise the writer will produce the weird utf-in-utf.
    auto consumer = NJson::CreateJsonConsumer(&outputStream, NYson::EYsonType::Node, jsonConfig);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("secrets")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("status").Value("ok")
                    .Item("value")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("key").Value(SecretKey)
                            .Item("value").Value(CyrillicValue)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("status").Value("ok")
        .EndMap();

    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    std::vector<ISecretVaultService::TSecretSubrequest> subrequests;
    subrequests.push_back({
        SecretId,
        SecretVersion,
        SecretDelegationToken,
        SecretSignature});

    auto subresponses = WaitFor(service->GetSecrets(subrequests));
    ASSERT_TRUE(subresponses.IsOK());
}

TEST_F(TDefaultSecretVaultTest, HighAscii)
{
    TStringStream outputStream;
    auto jsonConfig = New<NJson::TJsonFormatConfig>();
    jsonConfig->EncodeUtf8 = false; // Otherwise the writer will produce the weird utf-in-utf.
    auto consumer = NJson::CreateJsonConsumer(&outputStream, NYson::EYsonType::Node, jsonConfig);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("secrets")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("status").Value("ok")
                    .Item("value")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("key").Value(SecretKey)
                            .Item("value").Value(HighAsciiValue)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("status").Value("ok")
        .EndMap();

    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    std::vector<ISecretVaultService::TSecretSubrequest> subrequests;
    subrequests.push_back({
        SecretId,
        SecretVersion,
        SecretDelegationToken,
        SecretSignature});

    auto subresponses = WaitFor(service->GetSecrets(subrequests));
    ASSERT_TRUE(subresponses.IsOK());
}

TEST_F(TDefaultSecretVaultTest, GetToken)
{
    TStringStream outputStream;
    auto jsonConfig = New<NJson::TJsonFormatConfig>();
    jsonConfig->EncodeUtf8 = false;
    auto consumer = NJson::CreateJsonConsumer(&outputStream, NYson::EYsonType::Node, jsonConfig);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("status").Value("ok")
            .Item("token").Value("TheToken")
        .EndMap();
    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    ISecretVaultService::TDelegationTokenRequest request = {
        UserTicket,
        SecretId,
        SecretSignature,
        "a comment"
    };

    auto response = WaitFor(service->GetDelegationToken(request));
    ASSERT_TRUE(response.IsOK());
    ASSERT_EQ("TheToken", response.ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////
TEST_F(TDefaultSecretVaultTest, GetTokenFails)
{
    TStringStream outputStream;
    auto jsonConfig = New<NJson::TJsonFormatConfig>();
    jsonConfig->EncodeUtf8 = false;
    auto consumer = NJson::CreateJsonConsumer(&outputStream, NYson::EYsonType::Node, jsonConfig);
    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("status").Value("error")
            .Item("code").Value("access_error")
        .EndMap();
    consumer->Flush();

    SetCallback(outputStream.Str());

    auto service = CreateDefaultSecretVaultService();

    ISecretVaultService::TDelegationTokenRequest request = {
        UserTicket,
        SecretId,
        SecretSignature,
        "a comment"
    };

    auto response = WaitFor(service->GetDelegationToken(request));
    ASSERT_FALSE(response.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
