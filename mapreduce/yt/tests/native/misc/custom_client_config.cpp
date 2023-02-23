#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(CustomClientConfig)
{
    Y_UNIT_TEST(TestRetries)
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
            UNIT_ASSERT_EXCEPTION(client->Set(workingDir + "/table/@my_attr", 42), TAbortedForTestPurpose);
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage("/set", config->RetryCount - 1);
            UNIT_ASSERT_NO_EXCEPTION(client->Set(workingDir + "/table/@my_attr", -43));
        }
    }

    Y_UNIT_TEST(TestTransactionAutoPing)
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
            UNIT_ASSERT(pt1 != pt2);
        }

        {
            TStartTransactionOptions opts;
            opts.AutoPingable(false);
            auto transaction = client->StartTransaction(opts);

            const auto pt1 = GetLastPingTime(client, transaction);
            Sleep(TDuration::Seconds(1));
            const auto pt2 = GetLastPingTime(client, transaction);
            UNIT_ASSERT_VALUES_EQUAL(pt1, pt2);

            transaction->Ping();
            const auto pt3 = GetLastPingTime(client, transaction);
            UNIT_ASSERT(pt1 != pt3);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
