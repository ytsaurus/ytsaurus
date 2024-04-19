#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/getpid.h>


using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

static TInstant GetLastPingTime(const IClientBasePtr& client, const ITransactionPtr& tx)
{
    auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@last_ping_time");
    return TInstant::ParseIso8601(node.AsString());
};

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

    Y_UNIT_TEST(TestDeadline)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto transaction = client->StartTransaction(
            TStartTransactionOptions().Deadline(TInstant::Now() + TDuration::Seconds(1)));

        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transaction->GetId())));

        Sleep(TDuration::Seconds(7));

        UNIT_ASSERT(!client->Exists("#" + GetGuidAsString(transaction->GetId())));

    }

    Y_UNIT_TEST(TestDetachAttach)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->PingInterval = TDuration::MilliSeconds(100);

        TTransactionId transactionId;
        {
            auto transaction = client->StartTransaction(
                TStartTransactionOptions().Timeout(TDuration::Seconds(10)));
            transactionId = transaction->GetId();
            transaction->Detach();
        }

        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transactionId)));

        {
            auto transaction = client->AttachTransaction(transactionId);
            auto oldPingTime = GetLastPingTime(client, transaction);
            Sleep(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(oldPingTime, GetLastPingTime(client, transaction));
            transaction->Detach();
        }

        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transactionId)));

        {
            auto transaction = client->AttachTransaction(
                transactionId,
                TAttachTransactionOptions().AutoPingable(true));
            auto oldPingTime = GetLastPingTime(client, transaction);
            Sleep(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_UNEQUAL(oldPingTime, GetLastPingTime(client, transaction));
            transaction->Detach();
        }

        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transactionId)));
        Sleep(TDuration::Seconds(12));
        UNIT_ASSERT(!client->Exists("#" + GetGuidAsString(transactionId)));
    }

    Y_UNIT_TEST(TestPingErrors)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->PingInterval = TDuration::MilliSeconds(10);
        TConfig::Get()->UseAbortableResponse = true;

        auto transaction = client->StartTransaction(TStartTransactionOptions().Timeout(TDuration::Seconds(3)));
        auto transactionId = transaction->GetId();

        {
            auto outage = TAbortableHttpResponse::StartOutage("/ping");
            Sleep(TDuration::Seconds(1));
        }

        Sleep(TDuration::Seconds(5));
        UNIT_ASSERT(client->Exists("#" + GetGuidAsString(transactionId)));
    }
}

////////////////////////////////////////////////////////////////////////////////
