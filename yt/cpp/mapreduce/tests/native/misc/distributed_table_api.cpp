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
