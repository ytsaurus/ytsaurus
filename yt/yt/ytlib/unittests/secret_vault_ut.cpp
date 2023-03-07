#include "mock_http_server.h"

#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/default_secret_vault_service.h>
#include <yt/ytlib/auth/secret_vault_service.h>
#include <yt/ytlib/auth/tvm_service.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/json/json_writer.h>
#include <yt/core/test_framework/framework.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NTests;

using ::testing::HasSubstr;

////////////////////////////////////////////////////////////////////////////////

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
        config->RequestTimeout = TDuration::MilliSeconds(100);
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

    void SetCallback(TMockHttpServer::TCallback callback)
    {
        MockHttpServer_.SetCallback(std::move(callback));
    }

private:
    class TMockTvmService
        : public ITvmService
    {
    public:
        virtual TFuture<TString> GetTicket(const TString& serviceId) override
        {
            return MakeFuture("ticket:" + serviceId);
        }
    };

    TMockHttpServer MockHttpServer_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDefaultSecretVaultTest, WarningResponseStatus)
{
    static const TString SecretId = "secret-id";
    static const TString SecretVersion = "secret-version";
    static const TString SecretDelegationToken = "secret-token";
    static const TString SecretSignature = "secret-signature";
    static const TString SecretKey = "secret-key";
    static const TString SecretValue = "secret-value";

    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/1/tokens/?consumer=yp.unittest"));

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
                            .EndMap()
                        .EndList()
                    .EndMap()
                .EndList()
                .Item("status").Value("ok")
            .EndMap();

        consumer->Flush();

        request->Output() << HttpResponse(200, outputStream.Str());
    });

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

    const auto& secrets = subresponses[0].ValueOrThrow().Payload;
    ASSERT_EQ(1, secrets.size());

    auto it = secrets.find(SecretKey);
    ASSERT_TRUE(secrets.end() != it);
    ASSERT_EQ(SecretValue, it->second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
