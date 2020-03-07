#include "mock_http_server.h"

#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/default_tvm_service.h>
#include <yt/ytlib/auth/helpers.h>
#include <yt/ytlib/auth/tvm_service.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/test_framework/framework.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NTests;
using namespace NYTree;
using namespace NYson;

using ::testing::AllOf;
using ::testing::AnyOf;
using ::testing::HasSubstr;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::Throw;
using ::testing::_;

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmTest
    : public ::testing::Test
{
protected:
    TDefaultTvmServiceConfigPtr CreateDefaultTvmServiceConfig()
    {
        auto config = New<TDefaultTvmServiceConfig>();
        config->Host = MockHttpServer_.IsStarted() ? MockHttpServer_.GetHost() : "localhost";
        config->Port = MockHttpServer_.IsStarted() ? MockHttpServer_.GetPort() : static_cast<ui16>(0);
        config->Token = "IAmATvmToken";
        config->Src = "TheSource";
        config->RequestTimeout = TDuration::MilliSeconds(100);
        return config;
    }

    ITvmServicePtr CreateDefaultTvmService(TDefaultTvmServiceConfigPtr config = {})
    {
        return NAuth::CreateDefaultTvmService(
            config ? config : CreateDefaultTvmServiceConfig(),
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

    TErrorOr<TString> MakeRequest(ITvmServicePtr service)
    {
        return service->GetTicket("TheDestination").Get();
    }

    void CheckRequest(TClientRequest* request)
    {
        EXPECT_THAT(
            request->Input().FirstLine(),
            HasSubstr("/tvm/tickets?src=TheSource&dsts=TheDestination"));
        EXPECT_EQ("IAmATvmToken", request->Input().Headers().FindHeader("Authorization")->Value());
    }

    TMockHttpServer MockHttpServer_;
};

TEST_F(TDefaultTvmTest, FailOnBadHost)
{
    auto config = CreateDefaultTvmServiceConfig();
    config->Host = "lokalhozd";
    config->Port = 1;
    auto service = CreateDefaultTvmService(config);
    auto result = service->GetTicket("TheDestination").Get();
    ASSERT_TRUE(!result.IsOK());
    // Sometimes DNS is slow causing flaky tests.
    EXPECT_THAT(
        CollectMessages(result),
        AnyOf(HasSubstr("Operation timed out"), HasSubstr("DNS resolve failed")));
}

TEST_F(TDefaultTvmTest, FailOn5xxResponse)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(500, "");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("TVM call returned HTTP status code 500"));
}

TEST_F(TDefaultTvmTest, FailOn4xxResponse)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(404, "");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("TVM call returned HTTP status code 404"));
}

TEST_F(TDefaultTvmTest, FailOnEmptyResponse)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(200, "");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TDefaultTvmTest, FailOnMalformedResponse)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(200, "#$&(^$#@(^");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TDefaultTvmTest, FailOnError)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(
            200,
            R"jj({"TheDestination":{"error": "Nope", "tvm_id": 42}})jj");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing TVM daemon reply"));
}

TEST_F(TDefaultTvmTest, Success)
{
    SetCallback([&] (TClientRequest* request) {
        CheckRequest(request);
        request->Output() << HttpResponse(
            200,
            R"jj({"TheDestination":{"ticket": "IAmATvmTicket", "tvm_id": 42}})jj");
    });
    auto service = CreateDefaultTvmService();
    auto result = MakeRequest(service);
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("IAmATvmTicket", result.ValueOrThrow());
}

TEST_F(TDefaultTvmTest, NoSrc)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(
            request->Input().FirstLine(),
            HasSubstr("/tvm/tickets?dsts=TheDestination"));
        EXPECT_EQ("IAmATvmToken", request->Input().Headers().FindHeader("Authorization")->Value());
        request->Output() << HttpResponse(
            200,
            R"jj({"TheDestination":{"ticket": "IAmATvmTicket", "tvm_id": 42}})jj");
    });
    auto config = CreateDefaultTvmServiceConfig();
    config->Src.clear();
    auto service = CreateDefaultTvmService(config);
    auto result = MakeRequest(service);
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("IAmATvmTicket", result.ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
