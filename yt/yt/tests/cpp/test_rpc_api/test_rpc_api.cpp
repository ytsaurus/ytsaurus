//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DRIVER_BACKENDS=['rpc']
//%ENABLE_RPC_PROXY=True
//%DELTA_MASTER_CONFIG={"object_service":{"timeout_backoff_lead_time":100}}

#include <yt/yt/tests/cpp/modify_rows_test.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rpc_proxy/transaction_impl.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/ytree/convert.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TString TryGetStickyProxyAddress(const ITransactionPtr& transaction)
{
    return transaction
        ->As<NRpcProxy::TTransaction>()
        ->GetStickyProxyAddress();
}

TString GetStickyProxyAddress(const ITransactionPtr& transaction)
{
    auto address = TryGetStickyProxyAddress(transaction);
    EXPECT_TRUE(address);
    return address;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TApiTestBase, TestDuplicateTransactionId)
{
    TTransactionStartOptions options{
        .Id = MakeRandomId(EObjectType::AtomicTabletTransaction, MinValidCellTag)
    };

    auto transaction1 = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet, options))
        .ValueOrThrow();

    bool found = false;
    // There are several proxies in the environment and
    // the only one of them will return the error,
    // so try start several times to catch it.
    for (int i = 0; i < 32; ++i) {
        auto resultOrError = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet, options));
        if (resultOrError.IsOK()) {
            auto transaction2 = resultOrError
                .Value();
            EXPECT_FALSE(GetStickyProxyAddress(transaction1) == GetStickyProxyAddress(transaction2));
        } else {
            EXPECT_FALSE(NRpcProxy::IsRetriableError(resultOrError));
            found = true;
        }
    }
    EXPECT_TRUE(found);

    WaitFor(transaction1->Commit())
        .ValueOrThrow();
}

TEST_F(TApiTestBase, TestStartTimestamp)
{
    auto timestamp = WaitFor(Client_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    TTransactionStartOptions options{
        .StartTimestamp = timestamp
    };

    auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet, options))
        .ValueOrThrow();

    EXPECT_EQ(timestamp, transaction->GetStartTimestamp());
}

TEST_F(TApiTestBase, TestTransactionProxyAddress)
{
    // Prepare for tests: discover some proxy address.
    auto proxyAddress = GetStickyProxyAddress(WaitFor(Client_->StartTransaction(
        NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow());
    // Tablet transaction supports sticky proxy address.
    {
        auto transaction = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        EXPECT_TRUE(TryGetStickyProxyAddress(transaction));
    }
    // Master transaction does not support sticky proxy address.
    {
        auto transaction = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();
        EXPECT_FALSE(TryGetStickyProxyAddress(transaction));
    }
    // Attachment to master transaction with specified sticky proxy address is not supported.
    {
        auto transaction = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();

        TTransactionAttachOptions attachOptions{.StickyAddress = proxyAddress};
        EXPECT_THROW(Client_->AttachTransaction(transaction->GetId(), attachOptions), TErrorException);

        // Sanity check.
        Client_->AttachTransaction(transaction->GetId());
    }
    // Attached tablet transaction must be recognized as sticky (in particular, must support sticky proxy address)
    // even if sticky address option has been not provided during attachment explicitly.
    {
        auto transaction = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        bool found = false;
        // Try attach several times to choose proper proxy implicitly.
        for (int i = 0; i < 32; ++i) {
            ITransactionPtr transaction2;
            try {
                transaction2 = Client_->AttachTransaction(transaction->GetId());
            } catch (const std::exception&) {
                continue;
            }
            EXPECT_EQ(GetStickyProxyAddress(transaction), GetStickyProxyAddress(transaction2));
            found = true;
        }
        EXPECT_TRUE(found);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TModifyRowsTest, TestAttachTabletTransaction)
{
    auto transaction = WaitFor(Client_->StartTransaction(
        NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    auto proxyAddress = GetStickyProxyAddress(transaction);

    // Sanity check that the environment contains at least two proxies
    // and that the transaction start changes target proxy over time.
    {
        bool foundSecondProxy = false;
        for (int i = 0; i < 32; ++i) {
            auto transaction2 = WaitFor(Client_->StartTransaction(
                NTransactionClient::ETransactionType::Tablet))
                .ValueOrThrow();
            if (GetStickyProxyAddress(transaction2) != proxyAddress) {
                foundSecondProxy = true;
                break;
            }
        }
        EXPECT_TRUE(foundSecondProxy);
    }

    TTransactionAttachOptions attachOptions{.StickyAddress = proxyAddress};

    // Transaction attachment.
    auto transaction2 = Client_->AttachTransaction(
        transaction->GetId(),
        attachOptions);
    EXPECT_EQ(proxyAddress, GetStickyProxyAddress(transaction2));
    EXPECT_EQ(transaction->GetId(), transaction2->GetId());

    auto transaction3 = Client_->AttachTransaction(
        transaction->GetId(),
        attachOptions);
    EXPECT_EQ(proxyAddress, GetStickyProxyAddress(transaction3));
    EXPECT_EQ(transaction->GetId(), transaction3->GetId());

    // Concurrent writes.
    WriteSimpleRow(transaction, 0, 10, /*sequenceNumber*/ std::nullopt);
    WriteSimpleRow(transaction2, 1, 11, /*sequenceNumber*/ std::nullopt);
    WriteSimpleRow(transaction, 2, 12, /*sequenceNumber*/ std::nullopt);
    WriteSimpleRow(transaction2, 3, 13, /*sequenceNumber*/ std::nullopt);

    WaitFor(transaction->Flush())
        .ValueOrThrow();

    ValidateTableContent({});

    WaitFor(transaction2->Commit())
        .ValueOrThrow();

    ValidateTableContent({
        {0, 10},
        {1, 11},
        {2, 12},
        {3, 13},
    });

    // Double-commit.
    WriteSimpleRow(transaction3, 4, 14, /*sequenceNumber*/ std::nullopt);
    EXPECT_THROW(WaitFor(transaction3->Commit()).ValueOrThrow(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TModifyRowsTest, TestReordering)
{
    const int rowCount = 20;

    for (int i = 0; i < rowCount; ++i) {
        WriteSimpleRow(i, i + 10);
        WriteSimpleRow(i, i + 11);
    }
    SyncCommit();

    std::vector<std::pair<i64, i64>> expected;
    for (int i = 0; i < rowCount; ++i) {
        expected.emplace_back(i, i + 11);
    }
    ValidateTableContent(expected);
}

TEST_F(TModifyRowsTest, TestIgnoringSeqNumbers)
{
    WriteSimpleRow(0, 10, 4);
    WriteSimpleRow(1, 11, 3);
    WriteSimpleRow(0, 12, 2);
    WriteSimpleRow(1, 13, -1);
    WriteSimpleRow(0, 14);
    WriteSimpleRow(1, 15, 100500);
    SyncCommit();

    ValidateTableContent({{0, 14}, {1, 15}});
}

////////////////////////////////////////////////////////////////////////////////

class TMultiLookupTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        auto configPath = TString(std::getenv("YT_DRIVER_CONFIG_PATH"));
        YT_VERIFY(configPath);
        IMapNodePtr config;
        {
            TIFStream configInStream(configPath);
            config = ConvertToNode(&configInStream)->AsMap();
        }
        config->AddChild("enable_multi_lookup", ConvertToNode(true));
        {
            TOFStream configOutStream(configPath);
            configOutStream << ConvertToYsonString(config).ToString() << Endl;
        }

        TDynamicTablesTestBase::SetUpTestCase();

        CreateTable(
            "//tmp/multi_lookup_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=v1;type=int64};]"
        );
    }

    static void TearDownTestCase()
    {
        TDynamicTablesTestBase::TearDownTestCase();
    }
};

TEST_F(TMultiLookupTest, TestMultiLookup)
{
    WriteUnversionedRow(
        {"k0", "v1"},
        "<id=0> 0; <id=1> 0;");
    WriteUnversionedRow(
        {"k0", "v1"},
        "<id=0> 1; <id=1> 1");

    auto key0 = PrepareUnversionedRow(
        {"k0", "v1"},
        "<id=0> 0;");
    auto key1 = PrepareUnversionedRow(
        {"k0", "v1"},
        "<id=0; ts=2> 1;");

    std::vector<TMultiLookupSubrequest> subrequests;
    subrequests.push_back({
        Table_,
        std::get<1>(key0),
        std::get<0>(key0),
        TLookupRowsOptions()});
    subrequests.push_back({
        Table_,
        std::get<1>(key1),
        std::get<0>(key1),
        TLookupRowsOptions()});

    auto rowsets = WaitFor(Client_->MultiLookup(
        subrequests,
        TMultiLookupOptions()))
        .ValueOrThrow();

    ASSERT_EQ(2u, rowsets.size());

    ASSERT_EQ(1u, rowsets[0]->GetRows().Size());
    ASSERT_EQ(1u, rowsets[1]->GetRows().Size());

    auto expected = ToString(YsonToSchemalessRow("<id=0> 0; <id=1> 0;"));
    auto actual = ToString(rowsets[0]->GetRows()[0]);
    EXPECT_EQ(expected, actual);

    expected = ToString(YsonToSchemalessRow("<id=0> 1; <id=1> 1;"));
    actual = ToString(rowsets[1]->GetRows()[0]);
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
