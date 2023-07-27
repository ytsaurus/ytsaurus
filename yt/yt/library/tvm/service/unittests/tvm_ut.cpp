#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/testing/common/env.h>

#include <util/system/fs.h>

namespace NYT::NAuth {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString USER_TICKET =
    "3:user:CAwQ__________9_GhcKAgh7EHsaB2ZvbzpiYXIg0oXYzAQoAg:HtBvtdGQBlIFtsg0bj9qmks1qeNdRQG1k"
    "ur-f5f_TTFWAtY1QtV_ak9MYKTLdAK3t_KKxBB-yx0NUWFdOe8gDDDqwgREId8CxwWODmm6vwiA7F10Q1XR-aO4mVnn"
    "x03UqBPKzoqPxIFHigcIGWzTli8BCe7iGmD9K4FxOra1PRc";

static const TString SERVICE_TICKET =
    "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5Y"
    "jWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1"
    "SjhLyAk1b_zJsx8viRAhCU";

static const TString CACHE_PATH = "./ticket_parser_cache/";

////////////////////////////////////////////////////////////////////////////////

class TTvmTest
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

    TTvmServiceConfigPtr CreateTvmServiceConfig()
    {
        auto config = New<TTvmServiceConfig>();
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

    ITvmServicePtr CreateTvmService(
        TTvmServiceConfigPtr config = {}, bool populateCache = true)
    {
        if (populateCache) {
            PopulateCache();
        }
        return NAuth::CreateTvmService(
            config ? config : CreateTvmServiceConfig());
    }
};

TEST_F(TTvmTest, FetchesSelfId)
{
    auto service = CreateTvmService();
    auto result = service->GetSelfTvmId();
    EXPECT_EQ(100500u, result);
}

TEST_F(TTvmTest, FetchesServiceTicket)
{
    auto service = CreateTvmService();
    auto result = service->GetServiceTicket("TheDestination");
    EXPECT_EQ("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", result);

    EXPECT_THROW(service->GetServiceTicket("AnotherDestination"), std::exception);
}

TEST_F(TTvmTest, DoesNotFetchServiceTicketWhenDisabled)
{
    auto config = CreateTvmServiceConfig();
    config->ClientEnableServiceTicketFetching = false;
    auto service = CreateTvmService(config);
    EXPECT_THROW(service->GetServiceTicket("TheDestination"), TErrorException);
}

TEST_F(TTvmTest, ParsesUserTicket)
{
    auto service = CreateTvmService();
    auto result = service->ParseUserTicket(USER_TICKET);
    EXPECT_EQ(123u, result.DefaultUid);
    EXPECT_EQ(THashSet<TString>{"foo:bar"}, result.Scopes);
    EXPECT_THROW(service->ParseUserTicket("BadTicket"), TErrorException);
}

TEST_F(TTvmTest, DoesNotParseUserTicketWhenDisabled)
{
    auto config = CreateTvmServiceConfig();
    config->ClientEnableUserTicketChecking = false;
    auto service = CreateTvmService(config);
    EXPECT_THROW(service->ParseUserTicket(USER_TICKET), TErrorException);
}

TEST_F(TTvmTest, ParsesServiceTicket)
{
    auto config = CreateTvmServiceConfig();
    config->ClientEnableUserTicketChecking = false;
    config->ClientEnableServiceTicketFetching = false;
    config->ClientEnableServiceTicketChecking = true;
    auto service = CreateTvmService(config);
    auto result = service->ParseServiceTicket(SERVICE_TICKET);
    EXPECT_EQ(123u, result.TvmId);
    EXPECT_THROW(service->ParseServiceTicket("BadTicket"), TErrorException);
}

TEST_F(TTvmTest, DoesNotParseServiceTicketWhenDisabled)
{
    auto config = CreateTvmServiceConfig();
    config->ClientEnableUserTicketChecking = false;
    config->ClientEnableServiceTicketFetching = false;
    config->ClientEnableServiceTicketChecking = false;
    auto service = CreateTvmService(config);
    EXPECT_THROW(service->ParseServiceTicket(SERVICE_TICKET), TErrorException);
}

TEST_F(TTvmTest, InitializationFailure)
{
    EXPECT_THROW(
        CreateTvmService(CreateTvmServiceConfig(), false),
        std::exception);
}

} // namespace
} // namespace NYT::NAuth
