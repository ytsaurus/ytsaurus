#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TTestDistributedWriteTableFixture
    : public TTestFixture
{
public:
    TTestDistributedWriteTableFixture()
        : TTestFixture()
    {
        GetClient()->Create(GetTablePath(), ENodeType::NT_TABLE);
    }

    TYPath GetTablePath() const
    {
        static const TString TableName = "/distributed_table";
        return GetWorkingDir() + TableName;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(DistributedWriteTable, StartPingFinishSession)
{
    TTestDistributedWriteTableFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetTablePath();

    auto session = client->StartDistributedWriteTableSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    client->PingDistributedWriteTableSession(session.Session_);

    client->FinishDistributedWriteTableSession(session.Session_, /*results*/ {});
}

TEST(DistributedWriteTable, WriteNode)
{
    TTestDistributedWriteTableFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetTablePath();

    auto session = client->StartDistributedWriteTableSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    TNode row;
    row["value"] = 1;

    {
        const auto& cookie = session.Cookies_[0];
        auto writer = client->CreateTableFragmentWriter<TNode>(cookie);

        writer->AddRow(row);
        writer->Finish();

        auto writeResult = writer->GetWriteFragmentResult();

        client->FinishDistributedWriteTableSession(session.Session_, /*results*/ {writeResult});
    }

    auto reader = client->CreateTableReader<TNode>(path);

    EXPECT_EQ(reader->GetRow(), row);
}

TEST(DistributedWriteTable, WriteYaMR)
{
    TTestDistributedWriteTableFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetTablePath();

    auto session = client->StartDistributedWriteTableSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    TYaMRRow row;
    row.Key = "test_key";
    row.SubKey = "test_subkey";
    row.Value = "test_value";

    {
        const auto& cookie = session.Cookies_[0];
        auto writer = client->CreateTableFragmentWriter<TYaMRRow>(cookie);

        writer->AddRow(row);
        writer->Finish();

        auto writeResult = writer->GetWriteFragmentResult();

        client->FinishDistributedWriteTableSession(session.Session_, /*results*/ {writeResult});
    }

    auto reader = client->CreateTableReader<TYaMRRow>(path);

    auto actualRow = reader->GetRow();
    EXPECT_EQ(actualRow.Key, row.Key);
    EXPECT_EQ(actualRow.SubKey, row.SubKey);
    EXPECT_EQ(actualRow.Value, row.Value);
}

TEST(DistributedWriteTable, WithTransaction)
{
    TTestDistributedWriteTableFixture fixture;

    auto client = fixture.GetClient();
    auto tx = client->StartTransaction();
    auto path = fixture.GetTablePath();

    auto session = tx->StartDistributedWriteTableSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    TNode row;
    row["value"] = 1;

    {
        const auto& cookie = session.Cookies_[0];
        auto writer = tx->CreateTableFragmentWriter<TNode>(cookie);

        writer->AddRow(row);
        writer->Finish();

        auto writeResult = writer->GetWriteFragmentResult();

        client->FinishDistributedWriteTableSession(session.Session_, /*results*/ {writeResult});
    }

    tx->Commit();

    auto reader = client->CreateTableReader<TNode>(path);

    EXPECT_EQ(reader->GetRow(), row);
}

TEST(DistributedWriteTable, ExceedTimeout)
{
    // NB(achains): Or remove and run with -DYT_RECIPE_BUILD_FROM_SOURCE=1
    SKIP_TEST_IF(true, "Enable test after YT-27085 deploy");

    TTestDistributedWriteTableFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetTablePath();

    TStartDistributedWriteTableOptions options;
    options.SessionTimeout(TDuration::Seconds(1));

    auto session = client->StartDistributedWriteTableSession(path, /*cookieCount*/ 1, options);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    Sleep(TDuration::Seconds(2));

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        client->FinishDistributedWriteTableSession(session.Session_, {}),
        NYT::TErrorResponse,
        "No such transaction");
}

////////////////////////////////////////////////////////////////////////////////
