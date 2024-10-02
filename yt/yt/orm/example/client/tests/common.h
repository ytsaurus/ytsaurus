#include <yt/yt/orm/example/client/native/autogen/client.h>

#include <yt/yt/orm/client/native/client.h>
#include <yt/yt/orm/client/native/helpers.h>
#include <yt/yt/orm/client/native/request.h>
#include <yt/yt/orm/client/native/response.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NOrm::NExample::NClient::NTests {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NConcurrency;
using namespace NYT::NOrm::NClient::NNative;
using namespace NYT::NOrm::NClient::NObjects;
using namespace NYT::NOrm::NExample::NClient::NNative;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger Logger;

////////////////////////////////////////////////////////////////////////////////

class TNativeClientTestSuite
    : public ::testing::Test
{
public:
    void SetUp() override;

    const IClientPtr& GetClient() const;

    int GetExpectedMasterCount() const;

    void WaitAllMastersAlive() const;

    static constexpr i64 BookId2Always_ = 2;

protected:
    IClientPtr Client_;

    static IClientPtr CreateClient();
};

////////////////////////////////////////////////////////////////////////////////

class TGenericOrmClientTest
    : public ::testing::Test
{
public:
    void SetUp() override;

    NRpc::IRoamingChannelProviderPtr GetChannelProvider();

protected:
    NRpc::IRoamingChannelProviderPtr ChannelProvider_;
};

////////////////////////////////////////////////////////////////////////////////

bool CheckObjectExistence(
    const IClientPtr& client,
    TObjectTypeValue objectType,
    const TString& objectId);

void CreateUser(
    const IClientPtr& client,
    TTransactionId transactionId,
    const TString& userId);

void CreatePublisher(
    const IClientPtr& client,
    i64 publisherId,
    TTransactionId transactionId = TTransactionId{});

TObjectIdentity CreateBook(
    const IClientPtr& client,
    i64 publisherId,
    i64 bookId,
    bool released,
    std::optional<TUpdateIfExisting> updateIfExisting = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

class TTestTable
{
public:
    struct TRow
    {
        TString Key;
        TString Value;

        bool operator == (const TRow& other) const;
    };

    static TTestTable Create(
        const NApi::IClientPtr& ytClient);

    void InsertRow(
        const NApi::ITransactionPtr& ytTransaction,
        const TString& key,
        const TString& value);

    std::vector<TRow> Select(
        const NApi::IClientPtr& ytClient);

private:
    const TString TablePath_;

    explicit TTestTable(TString tablePath);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NTests
