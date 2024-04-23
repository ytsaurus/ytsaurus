#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/threading/future/async.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <util/thread/pool.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

static TInstant GetLastPingTime(const IClientBasePtr& client, const ITransactionPtr& tx)
{
    auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@last_ping_time");
    return TInstant::ParseIso8601(node.AsString());
};

TEST(CustomClientConfig, TestRetries)
{
    TConfigPtr config = MakeIntrusive<TConfig>();
    config->RetryCount = 4;

    TTestFixture fixture(TCreateClientOptions().Config(config));

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->RetryCount = 6;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    client->Create(workingDir + "/table", NT_MAP);
    {
        auto outage = TAbortableHttpResponse::StartOutage("/set");
        EXPECT_THROW(client->Set(workingDir + "/table/@my_attr", 42), TAbortedForTestPurpose);
    }
    {
        auto outage = TAbortableHttpResponse::StartOutage("/set", config->RetryCount - 1);
        EXPECT_NO_THROW(client->Set(workingDir + "/table/@my_attr", -43));
    }
}

TEST(CustomClientConfig, TestTransactionAutoPing)
{
    auto config = MakeIntrusive<TConfig>();
    config->PingInterval = TDuration::MilliSeconds(100);

    TTestFixture fixture(TCreateClientOptions().Config(config));

    TConfig::Get()->PingInterval = TDuration::Seconds(5);

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto transaction = client->StartTransaction();

        const auto pt1 = GetLastPingTime(client, transaction);
        Sleep(TDuration::Seconds(1));
        const auto pt2 = GetLastPingTime(client, transaction);
        EXPECT_TRUE(pt1 != pt2);
    }

    {
        TStartTransactionOptions opts;
        opts.AutoPingable(false);
        auto transaction = client->StartTransaction(opts);

        const auto pt1 = GetLastPingTime(client, transaction);
        Sleep(TDuration::Seconds(1));
        const auto pt2 = GetLastPingTime(client, transaction);
        EXPECT_EQ(pt1, pt2);

        transaction->Ping();
        const auto pt3 = GetLastPingTime(client, transaction);
        EXPECT_TRUE(pt1 != pt3);
    }
}

////////////////////////////////////////////////////////////////////////////////
