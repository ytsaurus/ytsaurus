#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/getpid.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

static TInstant GetLastPingTime(const IClientBasePtr& client, const ITransactionPtr& tx)
{
    auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@last_ping_time");
    return TInstant::ParseIso8601(node.AsString());
};

TEST(Transactions, TestTitle)
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
        EXPECT_TRUE(title.find(pidStr) != TString::npos);
    }

    auto titleTx = client->StartTransaction(TStartTransactionOptions().Title("foo"));
    EXPECT_EQ(getTitle(titleTx), "foo");

    auto attrTitleTx = client->StartTransaction(TStartTransactionOptions().Attributes(TNode()("title", "bar")));
    EXPECT_EQ(getTitle(attrTitleTx), "bar");
}

TEST(Transactions, TestPing)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto transaction = client->StartTransaction();
    EXPECT_NO_THROW(transaction->Ping());

    auto attached = client->AttachTransaction(transaction->GetId());
    attached->Abort();

    EXPECT_THROW(transaction->Ping(), TErrorResponse);
}

TEST(Transactions, TestAutoPing)
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

TEST(Transactions, TestDeadline)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto transaction = client->StartTransaction(
        TStartTransactionOptions().Deadline(TInstant::Now() + TDuration::Seconds(1)));

    EXPECT_TRUE(client->Exists("#" + GetGuidAsString(transaction->GetId())));

    Sleep(TDuration::Seconds(7));

    EXPECT_TRUE(!client->Exists("#" + GetGuidAsString(transaction->GetId())));

}

TEST(Transactions, TestDetachAttach)
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

    EXPECT_TRUE(client->Exists("#" + GetGuidAsString(transactionId)));

    {
        auto transaction = client->AttachTransaction(transactionId);
        auto oldPingTime = GetLastPingTime(client, transaction);
        Sleep(TDuration::Seconds(3));
        EXPECT_EQ(oldPingTime, GetLastPingTime(client, transaction));
        transaction->Detach();
    }

    EXPECT_TRUE(client->Exists("#" + GetGuidAsString(transactionId)));

    {
        auto transaction = client->AttachTransaction(
            transactionId,
            TAttachTransactionOptions().AutoPingable(true));
        auto oldPingTime = GetLastPingTime(client, transaction);
        Sleep(TDuration::Seconds(3));
        EXPECT_NE(oldPingTime, GetLastPingTime(client, transaction));
        transaction->Detach();
    }

    EXPECT_TRUE(client->Exists("#" + GetGuidAsString(transactionId)));
    Sleep(TDuration::Seconds(12));
    EXPECT_TRUE(!client->Exists("#" + GetGuidAsString(transactionId)));
}

TEST(Transactions, TestPingErrors)
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
    EXPECT_TRUE(client->Exists("#" + GetGuidAsString(transactionId)));
}

////////////////////////////////////////////////////////////////////////////////
