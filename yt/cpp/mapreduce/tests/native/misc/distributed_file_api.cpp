#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TTestDistributedWriteFileFixture
    : public TTestFixture
{
public:
    TTestDistributedWriteFileFixture()
        : TTestFixture()
    {
        GetClient()->Create(GetFilePath(), ENodeType::NT_FILE);
    }

    TYPath GetFilePath() const
    {
        static const TString FileName = "/distributed_file";
        return GetWorkingDir() + FileName;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(DistributedWriteFile, StartPingFinishSession)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    client->PingDistributedWriteFileSession(session.Session_);

    client->FinishDistributedWriteFileSession(session.Session_, /*results*/ {});
}

TEST(DistributedWriteFile, SingleCookieSingleResult)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    auto& cookie = session.Cookies_[0];

    auto writer = client->CreateFileFragmentWriter(cookie);
    writer->Write("foo");
    writer->Finish();

    auto result = writer->GetWriteFragmentResult();

    client->FinishDistributedWriteFileSession(session.Session_, {result});

    {
        auto reader = client->CreateFileReader(path);
        EXPECT_EQ(reader->ReadAll(), "foo");
    }
}

TEST(DistributedWriteFile, MultipleCookieMultipleWriters)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 2);
    EXPECT_EQ(std::ssize(session.Cookies_), 2);

    TVector<TString> data = {"foo", "boo"};

    TVector<TWriteFileFragmentResult> results;
    results.reserve(data.size());

    for (int i = 0; i < std::ssize(session.Cookies_); ++i) {
        auto writer = client->CreateFileFragmentWriter(session.Cookies_[i]);
        writer->Write(data[i]);
        writer->Finish();
        results.push_back(writer->GetWriteFragmentResult());
    }

    client->FinishDistributedWriteFileSession(session.Session_, results);

    {
        auto reader = client->CreateFileReader(path);
        EXPECT_EQ(reader->ReadAll(), "fooboo");
    }
}

TEST(DistributedWriteFile, FinishReverseOrder)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 2);
    EXPECT_EQ(std::ssize(session.Cookies_), 2);

    TVector<TString> data = {"foo", "boo"};

    TVector<TWriteFileFragmentResult> results;
    results.reserve(data.size());

    for (int i = 0; i < std::ssize(session.Cookies_); ++i) {
        auto writer = client->CreateFileFragmentWriter(session.Cookies_[i]);
        writer->Write(data[i]);
        writer->Finish();
        results.push_back(writer->GetWriteFragmentResult());
    }

    client->FinishDistributedWriteFileSession(session.Session_, {results[1], results[0]});

    {
        auto reader = client->CreateFileReader(path);
        EXPECT_EQ(reader->ReadAll(), "boofoo");
    }
}

TEST(DistributedWriteFile, GetWriteResultNoFinish)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    auto& cookie = session.Cookies_[0];

    auto writer = client->CreateFileFragmentWriter(cookie);
    writer->Write("foo");

    EXPECT_THROW(writer->GetWriteFragmentResult(), TApiUsageError);
}

TEST(DistributedWriteFile, WithTransaction)
{
    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto tx = client->StartTransaction();

    auto path = fixture.GetFilePath();

    auto session = tx->StartDistributedWriteFileSession(path, /*cookieCount*/ 1);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    const auto& cookie = session.Cookies_[0];

    auto writer = tx->CreateFileFragmentWriter(cookie);
    writer->Write("foo");
    writer->Finish();

    auto result = writer->GetWriteFragmentResult();

    client->FinishDistributedWriteFileSession(session.Session_, {result});

    tx->Commit();

    {
        auto reader = client->CreateFileReader(path);
        EXPECT_EQ(reader->ReadAll(), "foo");
    }
}

TEST(DistributedWriteFile, ExceedTimeout)
{
    // NB(achains): Or remove and run with -DYT_RECIPE_BUILD_FROM_SOURCE=1
    SKIP_TEST_IF(true, "Enable test after YT-27085 deploy");

    TTestDistributedWriteFileFixture fixture;

    auto client = fixture.GetClient();
    auto path = fixture.GetFilePath();

    TStartDistributedWriteFileOptions options;
    options.SessionTimeout(TDuration::Seconds(1));

    auto session = client->StartDistributedWriteFileSession(path, /*cookieCount*/ 1, options);
    EXPECT_EQ(std::ssize(session.Cookies_), 1);

    Sleep(TDuration::Seconds(2));

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        client->FinishDistributedWriteFileSession(session.Session_, {}),
        NYT::TErrorResponse,
        "No such transaction");
}

////////////////////////////////////////////////////////////////////////////////
