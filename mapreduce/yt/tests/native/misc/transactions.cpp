#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>

#include <util/system/getpid.h>


using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(Transactions)
{
    Y_UNIT_TEST(TestTitle)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto getTitle = [&] (const ITransactionPtr& tx) {
            auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@title");
            return node.AsString();
        };

        auto noTitleTx = client->StartTransaction();
        {
            auto pidStr = ToString(GetPID());
            auto title = getTitle(noTitleTx);
            UNIT_ASSERT(title.find(pidStr) != TString::npos);
        }

        auto titleTx = client->StartTransaction(TStartTransactionOptions().Title("foo"));
        UNIT_ASSERT_VALUES_EQUAL(getTitle(titleTx), "foo");

        auto attrTitleTx = client->StartTransaction(TStartTransactionOptions().Attributes(TNode()("title", "bar")));
        UNIT_ASSERT_VALUES_EQUAL(getTitle(attrTitleTx), "bar");
    }

    Y_UNIT_TEST(TestPing)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto transaction = client->StartTransaction();
        UNIT_ASSERT_NO_EXCEPTION(transaction->Ping());

        auto attached = client->AttachTransaction(transaction->GetId());
        attached->Abort();

        UNIT_ASSERT_EXCEPTION(transaction->Ping(), TErrorResponse);
    }

    Y_UNIT_TEST(TestAutoPing)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->PingInterval = TDuration::MilliSeconds(100);

        auto getLastPingTime = [&] (const ITransactionPtr& tx) {
            auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@last_ping_time");
            return node.AsString();
        };

        {
            auto transaction = client->StartTransaction();

            const TString pt1 = getLastPingTime(transaction);
            Sleep(TDuration::Seconds(1));
            const TString pt2 = getLastPingTime(transaction);
            UNIT_ASSERT(pt1 != pt2);
        }

        {
            TStartTransactionOptions opts;
            opts.AutoPingable(false);
            auto transaction = client->StartTransaction(opts);

            const TString pt1 = getLastPingTime(transaction);
            Sleep(TDuration::Seconds(1));
            const TString pt2 = getLastPingTime(transaction);
            UNIT_ASSERT_VALUES_EQUAL(pt1, pt2);

            transaction->Ping();
            const TString pt3 = getLastPingTime(transaction);
            UNIT_ASSERT(pt1 != pt3);
        }
    }

    Y_UNIT_TEST(TestDeadline)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto transaction = client->StartTransaction(
            TStartTransactionOptions().Deadline(TInstant::Now() + TDuration::Seconds(4)));

        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transaction->GetId())));

        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT(!client->Exists("#" + GetGuidAsString(transaction->GetId())));

    }
}

////////////////////////////////////////////////////////////////////////////////
