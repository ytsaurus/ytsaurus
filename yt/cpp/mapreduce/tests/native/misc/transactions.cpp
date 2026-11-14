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

TEST(TTransactionsTest, Title)
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

TEST(TTransactionsTest, Ping)
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

TEST(TTransactionsTest, AutoPing)
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

TEST(TTransactionsTest, Deadline)
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

TEST(TTransactionsTest, DetachAttach)
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

TEST(TTransactionsTest, PingErrors)
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

TEST(TTransactionsTest, CommitOptions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    auto prerequisiteTx = client->StartTransaction();

    auto commitOptions = TCommitTransactionOptions()
        .AddPrerequisiteTransactionId(prerequisiteTx->GetId());

    // Prerequisite transaction is alive, commit must succeed.
    auto tx = client->StartTransaction();
    EXPECT_NO_THROW(tx->Commit(commitOptions));

    prerequisiteTx->Commit();

    // Prerequisite transaction is no longer alive, commit must fail.
    tx = client->StartTransaction();
    EXPECT_THROW(tx->Commit(commitOptions), yexception);
}

////////////////////////////////////////////////////////////////////////////////
