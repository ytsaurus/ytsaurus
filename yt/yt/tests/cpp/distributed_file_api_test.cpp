#include "distributed_file_api_test.h"

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/signature/signature.h>

namespace NYT::NCppTests {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TDistributedFileApiTest::SetUp()
{
    TApiTestBase::SetUp();

    static const TString Path = "//tmp/distributed_file_api_test";

    TCreateNodeOptions options;
    options.Force = true;

    WaitFor(Client_->CreateNode(Path, EObjectType::File, options))
        .ThrowOnError();

    File_ = Path;
}

TDistributedWriteFileSessionWithCookies TDistributedFileApiTest::StartDistributedWriteSession(
    int cookieCount,
    TTransactionId txId,
    std::optional<TDuration> timeout)
{
    TDistributedWriteFileSessionStartOptions options;
    options.CookieCount = cookieCount;
    options.SessionTimeout = timeout;
    if (!txId.IsEmpty()) {
        options.TransactionId = txId;
    }
    return WaitFor(Client_
        ->StartDistributedWriteFileSession(
            NYPath::TRichYPath(File_),
            options))
        .ValueOrThrow();
}

void TDistributedFileApiTest::PingDistributedWriteSession(
    const TSignedDistributedWriteFileSessionPtr& session)
{
    WaitFor(Client_->PingDistributedWriteFileSession(session))
        .ThrowOnError();
}

void TDistributedFileApiTest::FinishDistributedWriteSession(
    TSignedDistributedWriteFileSessionPtr session,
    std::vector<TSignedWriteFileFragmentResultPtr> results)
{
    WaitFor(Client_
        ->FinishDistributedWriteFileSession(
            TDistributedWriteFileSessionWithResults{
                .Session = std::move(session),
                .Results = std::move(results),
            }))
    .ThrowOnError();
}

NApi::TSignedWriteFileFragmentResultPtr TDistributedFileApiTest::DistributedWriteFile(
    const TSignedWriteFileFragmentCookiePtr& cookie,
    const TString& data)
{
    auto writer = Client_->CreateFileFragmentWriter(cookie);

    WaitFor(writer->Open())
        .ThrowOnError();

    auto dataRef = TSharedRef::FromString(data);

    WaitFor(writer->Write(dataRef)).ThrowOnError();
    WaitFor(writer->Close()).ThrowOnError();

    return writer->GetWriteFragmentResult();
}

TString TDistributedFileApiTest::ReadFile()
{
    auto reader = WaitFor(Client_->CreateFileReader(File_))
        .ValueOrThrow();

    auto sharedRef = reader->ReadAll();
    return TString(sharedRef.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

// Smoke tests

TEST_F(TDistributedFileApiTest, StartFinish)
{
    {
        auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 0);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 1);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 0);
        PingDistributedWriteSession(sessionWithCookies.Session);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }
}

TEST_F(TDistributedFileApiTest, StartCreateWriterFinish)
{
    auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 1);
    ASSERT_EQ(1, std::ssize(sessionWithCookies.Cookies));
    auto cookie = sessionWithCookies.Cookies[0];

    auto writer = Client_->CreateFileFragmentWriter(cookie);

    WaitFor(writer->Open())
        .ThrowOnError();

    WaitFor(writer->Close())
        .ThrowOnError();

    FinishDistributedWriteSession(sessionWithCookies.Session);
}

////////////////////////////////////////////////////////////////////////////////

// "Happy-path" basic cookie usage scenarios

TEST_F(TDistributedFileApiTest, SingleCookieSingleWrite)
{
    TString data = "DEADBEEF";

    auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 1);
    auto cookie = sessionWithCookies.Cookies[0];

    auto result = DistributedWriteFile(cookie, data);

    FinishDistributedWriteSession(
        std::move(sessionWithCookies.Session),
        {std::move(result)});

    auto fileContent = ReadFile();
    EXPECT_EQ(data, fileContent);
}

TEST_F(TDistributedFileApiTest, SingleCookieMultipleWrite)
{
    std::vector<TString> data = {"AAA", "BBB"};

    auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 1);
    auto cookie = sessionWithCookies.Cookies[0];

    std::vector<NApi::TSignedWriteFileFragmentResultPtr> results;
    results.reserve(data.size());

    for (const auto& fragment : data) {
        results.push_back(
            DistributedWriteFile(cookie, fragment));
    }

    FinishDistributedWriteSession(
        std::move(sessionWithCookies.Session),
        {std::move(results)});

    auto fileContent = ReadFile();
    auto concatenatedData = std::accumulate(data.begin(), data.end(), TString());
    EXPECT_EQ(concatenatedData, fileContent);
}

TEST_F(TDistributedFileApiTest, MultipleCookieMultipleWrite)
{
    std::vector<TString> data = {"AAA", "BBB"};

    auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 2);

    std::vector<NApi::TSignedWriteFileFragmentResultPtr> results;
    results.reserve(data.size());

    ASSERT_EQ(std::ssize(sessionWithCookies.Cookies), std::ssize(data));
    for (size_t i = 0; i < data.size(); ++i) {
        results.push_back(
            DistributedWriteFile(sessionWithCookies.Cookies[i], data[i]));
    }

    FinishDistributedWriteSession(
        std::move(sessionWithCookies.Session),
        {std::move(results)});

    auto fileContent = ReadFile();
    auto concatenatedData = std::accumulate(data.begin(), data.end(), TString());
    EXPECT_EQ(concatenatedData, fileContent);
}

////////////////////////////////////////////////////////////////////////////////

// Invalid usage

TEST_F(TDistributedFileApiTest, DuplicateResult)
{
    TString data = "DEADBEEF";

    auto sessionWithCookies = StartDistributedWriteSession(/*cookieCount*/ 1);
    auto cookie = sessionWithCookies.Cookies[0];

    auto result = DistributedWriteFile(cookie, data);

    EXPECT_THROW_WITH_SUBSTRING(FinishDistributedWriteSession(
        std::move(sessionWithCookies.Session),
        {result, result}), "Duplicate chunk list ids");
}

////////////////////////////////////////////////////////////////////////////////

// Transaction support

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
