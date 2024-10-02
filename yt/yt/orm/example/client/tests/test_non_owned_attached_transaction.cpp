#include "common.h"

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/transaction_impl.h>

#include <util/system/env.h>

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientNonOwnedAttachedTransactionTestSuite
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

    static NApi::IClientPtr CreateYTClient()
    {
        auto address = GetEnv("YT_HTTP_PROXY_ADDR");
        auto ytConfig = New<NApi::NRpcProxy::TConnectionConfig>();
        ytConfig->ConnectionType = NApi::EConnectionType::Rpc;
        ytConfig->ClusterUrl = address;

        return NApi::NRpcProxy::CreateConnection(ytConfig)->CreateClient(NApi::TClientOptions{});
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientNonOwnedAttachedTransactionTestSuite, Test)
{
    auto ytClient = GetYTClient();
    auto client = GetClient();
    auto table = TTestTable::Create(ytClient);
    auto ytTransaction = WaitFor(
        ytClient->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    table.InsertRow(ytTransaction, "a", "123");

    auto rpcProxyAddress = ytTransaction
        ->As<NApi::NRpcProxy::TTransaction>()
        ->GetStickyProxyAddress();
    EXPECT_TRUE(rpcProxyAddress);

    auto ormTransactionId = WaitFor(client->StartTransaction(
        TStartTransactionOptions{
            .UnderlyingTransactionId = ytTransaction->GetId(),
            .UnderlyingTransactionAddress = rpcProxyAddress,
        }))
        .ValueOrThrow()
        .TransactionId;

    CreateUser(client, ormTransactionId, "u2");

    EXPECT_FALSE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    EXPECT_EQ(0ul, table.Select(ytClient).size());

    WaitFor(client->CommitTransaction(ormTransactionId))
        .ThrowOnError();

    // Validate the transaction has been removed from ORM master.
    {
        auto updateResponseOrError = WaitFor(client->UpdateObject(
            "u2",
            TObjectTypeValues::User,
            /*updates*/ {},
            /*attributeTimestampPrerequisites*/ {},
            TUpdateObjectOptions{
                .TransactionId = ormTransactionId,
            }));
        EXPECT_FALSE(updateResponseOrError.IsOK());
        EXPECT_TRUE(updateResponseOrError.FindMatching(NOrm::NClient::EErrorCode::NoSuchTransaction));
    }

    EXPECT_FALSE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    EXPECT_EQ(0ul, table.Select(ytClient).size());

    table.InsertRow(ytTransaction, "b", "456");

    WaitFor(ytTransaction->Commit())
        .ThrowOnError();

    EXPECT_TRUE(CheckObjectExistence(client, TObjectTypeValues::User, "u2"));
    {
        std::vector<TTestTable::TRow> expectedRows{
            TTestTable::TRow{"a", "123"},
            TTestTable::TRow{"b", "456"},
        };
        EXPECT_EQ(expectedRows, table.Select(ytClient));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
