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

TEST_F(TDefaultTvmTest, FetchesSelfId)
{
    auto service = CreateDefaultTvmService();
    auto result = service->GetSelfTvmId();
    EXPECT_EQ(100500, result);
}

TEST_F(TDefaultTvmTest, FetchesServiceTicket)
{
    auto service = CreateDefaultTvmService();
    auto result = service->GetServiceTicket("TheDestination");
    EXPECT_EQ("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", result);

    EXPECT_THROW(service->GetServiceTicket("AnotherDestination"), std::exception);
}

TEST_F(TDefaultTvmTest, DoesNotFetchServiceTicketWhenDisabled)
{
    auto config = CreateDefaultTvmServiceConfig();
    config->ClientEnableServiceTicketFetching = false;
    auto service = CreateDefaultTvmService(config);
    EXPECT_THROW(service->GetServiceTicket("TheDestination"), TErrorException);
}

TEST_F(TDefaultTvmTest, ParsesUserTicket)
{
    auto service = CreateDefaultTvmService();
    auto result = service->ParseUserTicket(USER_TICKET);
    EXPECT_EQ(123, result.DefaultUid);
    EXPECT_EQ(THashSet<TString>{"foo:bar"}, result.Scopes);
    EXPECT_THROW(service->ParseUserTicket("BadTicket"), TErrorException);
}

TEST_F(TDefaultTvmTest, DoesNotParseUserTicketWhenDisabled)
{
    auto config = CreateDefaultTvmServiceConfig();
    config->ClientEnableUserTicketChecking = false;
    auto service = CreateDefaultTvmService(config);
    EXPECT_THROW(service->ParseUserTicket(USER_TICKET), TErrorException);
}

TEST_F(TDefaultTvmTest, InitializationFailure)
{
    EXPECT_THROW(
        CreateDefaultTvmService(CreateDefaultTvmServiceConfig(), false),
        std::exception);
}

} // namespace
} // namespace NYT::NAuth
