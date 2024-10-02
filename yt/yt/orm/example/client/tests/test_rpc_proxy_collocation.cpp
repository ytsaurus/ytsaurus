#include "common.h"

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>

#include <util/random/random.h>

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientRpcProxyCollocationTestSuite
    : public TNativeClientTestSuite
{
public:
    void SetUp() override
    {
        TNativeClientTestSuite::SetUp();

        YTClient_ = CreateYTClient();
    }

    const NApi::IClientPtr& GetYTClient() const
    {
        return YTClient_;
    }

private:
    NApi::IClientPtr YTClient_;

    NApi::IClientPtr CreateYTClient()
    {
        auto masters = WaitFor(Client_->GetMasters())
            .ValueOrThrow();
        EXPECT_EQ(std::ssize(masters.MasterInfos), GetExpectedMasterCount());

        auto address = masters.MasterInfos.front().RpcProxyAddress;
        EXPECT_FALSE(address.empty());

        auto ytConfig = New<NApi::NRpcProxy::TConnectionConfig>();
        ytConfig->ConnectionType = NApi::EConnectionType::Rpc;
        ytConfig->ProxyAddresses = {address};

        return NApi::NRpcProxy::CreateConnection(ytConfig)->CreateClient(NApi::TClientOptions{});
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientRpcProxyCollocationTestSuite, TestIndependentTransactions)
{
    auto ytClient = GetYTClient();
    auto client = GetClient();
    auto table = TTestTable::Create(ytClient);
    {
        auto ytTransaction = WaitFor(
            ytClient->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table.InsertRow(ytTransaction, "a", "123");
        WaitFor(ytTransaction->Commit())
            .ThrowOnError();
    }

    auto ormTransactionId = WaitFor(client->StartTransaction())
        .ValueOrThrow()
        .TransactionId;
    CreateUser(client, ormTransactionId, "u1");
    EXPECT_FALSE(CheckObjectExistence(client, TObjectTypeValues::User, "u1"));
    WaitFor(client->CommitTransaction(ormTransactionId))
        .ThrowOnError();
    EXPECT_TRUE(CheckObjectExistence(client, TObjectTypeValues::User, "u1"));

    {
        auto ytTransaction = WaitFor(
            ytClient->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table.InsertRow(ytTransaction, "b", "456");
        WaitFor(ytTransaction->Commit())
            .ThrowOnError();
    }
}

TEST_F(TNativeClientRpcProxyCollocationTestSuite, TestCommonTransaction)
{
    auto ytClient = GetYTClient();
    auto client = GetClient();
    auto table = TTestTable::Create(ytClient);
    auto ytTransaction = WaitFor(
        ytClient->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    table.InsertRow(ytTransaction, "a", "123");

    auto ormTransactionId = WaitFor(client->StartTransaction(
        TStartTransactionOptions{.UnderlyingTransactionId = ytTransaction->GetId()}))
        .ValueOrThrow()
        .TransactionId;

    CreateUser(client, ormTransactionId, "u2");

    EXPECT_FALSE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    EXPECT_EQ(0ul, table.Select(ytClient).size());

    WaitFor(client->CommitTransaction(ormTransactionId))
        .ThrowOnError();

    EXPECT_FALSE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    EXPECT_EQ(0ul, table.Select(ytClient).size());

    WaitFor(ytTransaction->Commit())
        .ThrowOnError();

    EXPECT_TRUE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    {
        std::vector<TTestTable::TRow> expectedRows{
            TTestTable::TRow{"a", "123"}
        };
        EXPECT_EQ(expectedRows, table.Select(ytClient));
    }
}

TEST_F(TNativeClientRpcProxyCollocationTestSuite, TouchBookFontIndex)
{
    auto publisherId = RandomNumber<ui64>(1000000000);
    CreatePublisher(Client_, publisherId);
    auto bookId1 = RandomNumber<ui64>(1000000000);
    auto bookId2 = RandomNumber<ui64>(1000000000);
    auto bookIdentity1 = CreateBook(Client_, publisherId, bookId1, /*released*/ false);
    auto bookIdentity2 = CreateBook(Client_, publisherId, bookId2, /*released*/ true);

    TString table("//home/example/db/books_by_font");
    TString query("font, publisher_id, book_id, book_id2 from [" + table + "]");

    auto ytClient = GetYTClient();
    auto rowset = WaitFor(ytClient->SelectRows(query))
        .ValueOrThrow()
        .Rowset;
    EXPECT_LE(2ul, rowset->GetRows().size());

    auto transaction = WaitFor(ytClient->StartTransaction(
        NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    transaction->DeleteRows(table,
        rowset->GetNameTable(),
        MakeSharedRange(rowset->GetRows(), std::move(rowset)));
    WaitFor(transaction->Commit())
        .ThrowOnError();
    EXPECT_EQ(0ul, WaitFor(ytClient->SelectRows(query))
        .ValueOrThrow()
        .Rowset->GetRows()
        .size());

    BatchTouchIndex(GetClient(),
        {bookIdentity1, bookIdentity2, TObjectIdentity("100500;2")},
        TObjectTypeValues::Book,
        "books_by_font",
        /*ignoreNonexistent*/ true);

    EXPECT_EQ(2ul, WaitFor(ytClient->SelectRows(query))
        .ValueOrThrow()
        .Rowset->GetRows()
        .size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
