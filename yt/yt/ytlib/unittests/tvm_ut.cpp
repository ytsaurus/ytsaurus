#include "mock_http_server.h"

#include <yt/yt/ytlib/auth/config.h>
#include <yt/yt/ytlib/auth/default_tvm_service.h>
#include <yt/yt/ytlib/auth/helpers.h>
#include <yt/yt/ytlib/auth/tvm_service.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/testing/common/env.h>

#include <util/system/fs.h>

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

static const TString USER_TICKET =
    "3:user:CAwQ__________9_GhcKAgh7EHsaB2ZvbzpiYXIg0oXYzAQoAg:HtBvtdGQBlIFtsg0bj9qmks1qeNdRQG1k"
    "ur-f5f_TTFWAtY1QtV_ak9MYKTLdAK3t_KKxBB-yx0NUWFdOe8gDDDqwgREId8CxwWODmm6vwiA7F10Q1XR-aO4mVnn"
    "x03UqBPKzoqPxIFHigcIGWzTli8BCe7iGmD9K4FxOra1PRc";

static const TString CACHE_PATH = "./ticket_parser_cache/";

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmTest
    : public ::testing::Test
{
protected:
    TDefaultTvmServiceConfigPtr CreateDefaultTvmServiceConfig()
    {
        auto config = New<TDefaultTvmServiceConfig>();
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

class TTicketParserTvmTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        NFs::MakeDirectoryRecursive(CACHE_PATH, NFs::FP_COMMON_FILE);
    }

    void TearDown() override
    {
        NFs::RemoveRecursive(CACHE_PATH);
    }

    void PopulateCache()
    {
        for (const auto file : { "public_keys", "service_tickets" }) {
            NFs::Copy(
                ArcadiaSourceRoot() + "/library/cpp/tvmauth/client/ut/files/" + file,
                CACHE_PATH + file);
        }
    }

    TDefaultTvmServiceConfigPtr CreateDefaultTvmServiceConfig()
    {
        auto config = New<TDefaultTvmServiceConfig>();
        config->ClientSelfId = 100500;
        config->ClientDiskCacheDir = CACHE_PATH;
        config->TvmHost = "https://localhost";
        config->TvmPort = 1;
        config->ClientEnableUserTicketChecking = true;
        config->ClientEnableServiceTicketFetching = true;
        config->ClientSelfSecret = "IAmATvmToken";
        config->ClientDstMap["TheDestination"] = 19;
        return config;
    }

    ITvmServicePtr CreateDefaultTvmService(
        TDefaultTvmServiceConfigPtr config = {}, bool populateCache = true)
    {
        if (populateCache) {
            PopulateCache();
        }
        return NAuth::CreateDefaultTvmService(
            config ? config : CreateDefaultTvmServiceConfig(),
            CreateThreadPoolPoller(1, "HttpPoller"));
    }
};

TEST_F(TTicketParserTvmTest, FetchesServiceTicket)
{
    auto service = CreateDefaultTvmService();
    auto result = service->GetTicket("TheDestination").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", result.ValueOrThrow());

    result = service->GetTicket("AnotherDestination").Get();
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TTicketParserTvmTest, DoesNotFetchServiceTicketWhenDisabled)
{
    auto config = CreateDefaultTvmServiceConfig();
    config->ClientEnableServiceTicketFetching = false;
    auto service = CreateDefaultTvmService(config);
    auto result = service->GetTicket("TheDestination").Get();
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TTicketParserTvmTest, ParsesUserTicket)
{
    auto service = CreateDefaultTvmService();
    auto result = service->ParseUserTicket(USER_TICKET);
    ASSERT_TRUE(result.IsOK());
    auto parsed = result.ValueOrThrow();
    EXPECT_EQ(123, parsed.DefaultUid);
    EXPECT_EQ(THashSet<TString>{"foo:bar"}, parsed.Scopes);
    result = service->ParseUserTicket("BadTicket");
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TTicketParserTvmTest, DoesNotParseUserTicketWhenDisabled)
{
    auto config = CreateDefaultTvmServiceConfig();
    config->ClientEnableUserTicketChecking = false;
    auto service = CreateDefaultTvmService(config);
    auto result = service->ParseUserTicket(USER_TICKET);
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TTicketParserTvmTest, InitializationFailure)
{
    EXPECT_THROW(
        CreateDefaultTvmService(CreateDefaultTvmServiceConfig(), false),
        std::exception);
}

} // namespace
} // namespace NYT::NAuth
