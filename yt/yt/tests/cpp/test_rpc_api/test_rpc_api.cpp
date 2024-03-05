#include <yt/yt/tests/cpp/modify_rows_test.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rpc_proxy/transaction_impl.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

#include <util/generic/cast.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYPath;
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

    // Independent writes from several sources.
    std::vector<std::pair<i64, i64>> expectedContent;

    for (int i = 0; i < 10; ++i) {
        WriteSimpleRow(transaction, 0 + i, 10 + i, /*sequenceNumber*/ std::nullopt);
        expectedContent.emplace_back(0 + i, 10 + i);
        WriteSimpleRow(transaction2, 100 + i, 110 + i, /*sequenceNumber*/ std::nullopt);
        expectedContent.emplace_back(100 + i, 110 + i);
    }

    // #FlushModifications as opposed to #Flush does not change the transaction state within RPC proxy
    // allowing to send modifications from the second transaction afterward.
    WaitFor(transaction->As<NRpcProxy::TTransaction>()->FlushModifications())
        .ThrowOnError();

    for (int i = 0; i < 10; ++i) {
        expectedContent.emplace_back(200 + i, 220 + i);
        WriteSimpleRow(transaction2, 200 + i, 220 + i, /*sequenceNumber*/ std::nullopt);
    }

    // Double-flush.
    EXPECT_THROW(WaitFor(transaction->As<NRpcProxy::TTransaction>()->FlushModifications()).ThrowOnError(), TErrorException);

    ValidateTableContent({});

    WaitFor(transaction2->Commit())
        .ValueOrThrow();

    ValidateTableContent(expectedContent);

    // Double-commit.
    WriteSimpleRow(transaction3, 4, 14, /*sequenceNumber*/ std::nullopt);
    EXPECT_THROW(WaitFor(transaction3->Commit()).ValueOrThrow(), TErrorException);
}

TEST_F(TModifyRowsTest, TestModificationsFlushedSignal)
{
    auto transaction = WaitFor(Client_->StartTransaction(
        NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow()
        ->As<NRpcProxy::TTransaction>();

    std::atomic<bool> flushed = false;
    transaction->SubscribeModificationsFlushed(BIND([&] {
        flushed = true;
    }));

    WaitFor(transaction->FlushModifications())
        .ThrowOnError();

    EXPECT_TRUE(flushed.load());
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

    auto results = WaitFor(Client_->MultiLookupRows(
        subrequests,
        TMultiLookupOptions()))
        .ValueOrThrow();

    ASSERT_EQ(2u, results.size());
    const auto& rowset0 = results[0].Rowset;
    const auto& rowset1 = results[1].Rowset;

    ASSERT_EQ(1u, rowset0->GetRows().Size());
    ASSERT_EQ(1u, rowset1->GetRows().Size());

    auto expected = ToString(YsonToSchemalessRow("<id=0> 0; <id=1> 0;"));
    auto actual = ToString(rowset0->GetRows()[0]);
    EXPECT_EQ(expected, actual);

    expected = ToString(YsonToSchemalessRow("<id=0> 1; <id=1> 1;"));
    actual = ToString(rowset1->GetRows()[0]);
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

class TClearTmpTestBase
    : public TApiTestBase
{
public:
    static void TearDownTestCase()
    {
        WaitFor(Client_->RemoveNode(TYPath("//tmp/*")))
            .ThrowOnError();

        TApiTestBase::TearDownTestCase();
    }
};

TEST_F(TClearTmpTestBase, TestAnyYsonValidation)
{
    TRichYPath tablePath("//tmp/test_any_validation");

    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"a", EValueType::Any}}));
    options.Force = true;

    // Empty yson.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedAnyValue("");
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        EXPECT_THROW_WITH_ERROR_CODE(
            WaitFor(writer->Close()).ThrowOnError(),
            NYT::NTableClient::EErrorCode::SchemaViolation);

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 0);
    }


    // Non-empty invalid yson.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedAnyValue("{foo");
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        EXPECT_THROW_WITH_ERROR_CODE(
            WaitFor(writer->Close()).ThrowOnError(),
            NYT::NTableClient::EErrorCode::SchemaViolation);

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 0);
    }

    // Composite value with invalid yson.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedCompositeValue("{foo");
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        EXPECT_THROW_WITH_ERROR_CODE(
            WaitFor(writer->Close()).ThrowOnError(),
            NYT::NTableClient::EErrorCode::SchemaViolation);

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 0);
    }

    // Valid value of another type should not be checked.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedInt64Value(42);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }
}

TEST_F(TClearTmpTestBase, TestAnyCompatibleTypes)
{
    TRichYPath tablePath("//tmp/test_any_compatible_types");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"a", EValueType::Any}}));
    options.Force = true;

    // Null.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedNullValue();
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Int64.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedInt64Value(1);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Uint64.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedUint64Value(1);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Boolean.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedBooleanValue(false);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Double.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedDoubleValue(4.2);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // String.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedStringValue("hello world!");
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Any.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto ysonString = ConvertToYsonString(42);
        auto value = MakeUnversionedAnyValue(ysonString.AsStringBuf());
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Composite.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedCompositeValue("[1; {a=1; b=2}]");
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 1);
    }

    // Min is not compatible.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedSentinelValue(EValueType::Min);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        EXPECT_THROW_WITH_ERROR_CODE(
            WaitFor(writer->Close()).ThrowOnError(),
            NYT::NTableClient::EErrorCode::SchemaViolation);

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 0);
    }

    // Max is not compatible.

    {
        WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName("a") == 0);

        auto value = MakeUnversionedSentinelValue(EValueType::Max);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        EXPECT_THROW_WITH_ERROR_CODE(
            WaitFor(writer->Close()).ThrowOnError(),
            NYT::NTableClient::EErrorCode::SchemaViolation);

        auto rowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(tablePath.GetPath() + "/@row_count"))
            .ValueOrThrow());
        EXPECT_EQ(rowCount, 0);
    }
}

TEST_F(TClearTmpTestBase, TestEmptyTableSkiffReading_YT18817)
{
    TRichYPath tablePath{"//tmp/test_empty_table_skiff_reading"};
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"a", EValueType::Int64}}));
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    for (int i = 0; i < 100; ++i) {
        auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
        auto req = apiServiceProxy.ReadTable();

        req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        auto format = BuildYsonStringFluently()
            .BeginAttributes()
                .Item("table_skiff_schemas")
                .BeginList()
                    .Item()
                    .BeginMap()
                        .Item("wire_type")
                        .Value("tuple")
                        .Item("children")
                        .BeginList()
                            .Item()
                            .BeginMap()
                                .Item("wire_type")
                                .Value("int64")
                                .Item("name")
                                .Value("a")
                            .EndMap()
                        .EndList()
                    .EndMap()
                .EndList()
            .EndAttributes()
            .Value("skiff");

        req->set_format(format.ToString());

        ToProto(req->mutable_path(), tablePath);
        auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
            .ValueOrThrow();

        stream->ReadAll();
    }
}

TEST_F(TClearTmpTestBase, TestErroneousSkiffReading_YTADMINREQ_32428)
{
    TRichYPath tablePath{"//tmp/test_errorneous_skiff_reading"};
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    // options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"a", EValueType::Int64}}));
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto aColumnId = writer->GetNameTable()->GetIdOrRegisterName("a");

        auto value = MakeUnversionedInt64Value(1, aColumnId);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        std::vector<TUnversionedRow> rows;
        rows.push_back(owningRow);
        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();
    }


    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    auto format = BuildYsonStringFluently()
        .BeginAttributes()
            .Item("table_skiff_schemas")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("wire_type")
                    .Value("tuple")
                    .Item("children")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("string32")
                            .Item("name")
                            .Value("a")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("skiff");

    req->set_format(format.ToString());

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    EXPECT_THROW_WITH_SUBSTRING(stream->ReadAll(), "Unexpected type of");
}

TEST_F(TClearTmpTestBase, FordiddenFormat_YT_20098)
{
    TString name = "foo";
    if (!WaitFor(Client_->NodeExists("//sys/users/" + name)).ValueOrThrow()) {
        TCreateObjectOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", name);
        options.Attributes = std::move(attributes);
        WaitFor(Client_->CreateObject(NObjectClient::EObjectType::User, options))
            .ThrowOnError();
    }

    auto clientOptions = TClientOptions::FromUser(name);
    auto client_ = Connection_->CreateClient(clientOptions);

    THashMap<TString, bool> enableFalse;
    enableFalse["enable"] = false;
    THashMap<TString, THashMap<TString, bool>> disableArrow;
    disableArrow["arrow"] = enableFalse;
    THashMap<TString, THashMap<TString, THashMap<TString, bool>>> formatsConf;
    formatsConf["formats"] = disableArrow;

    WaitFor(Client_->SetNode("//sys/rpc_proxies/@config", ConvertToYsonString(formatsConf))).ThrowOnError();

    Sleep(TDuration::Seconds(0.5));

    TRichYPath tablePath("//tmp/forbidden_format");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"IntColumn", EValueType::Int64}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;

    WaitFor(client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto columnId = writer->GetNameTable()->GetIdOrRegisterName("IntColumn");

        auto value = MakeUnversionedInt64Value(1, columnId);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        YT_VERIFY(writer->Write({owningRow}));
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    auto format = BuildYsonStringFluently().Value("arrow");

    req->set_format(format.ToString());

    ToProto(req->mutable_path(), tablePath);

    EXPECT_THROW_WITH_ERROR_CODE(WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow(), NYT::NApi::EErrorCode::FormatDisabled);

}

////////////////////////////////////////////////////////////////////////////////

class TArrowTestBase
    : public TClearTmpTestBase
{ };

std::shared_ptr<arrow::RecordBatch> MakeBatch(TStringBuf buf)
{
    auto buffer = arrow::Buffer(reinterpret_cast<const ui8*>(buf.data()), buf.size());
    arrow::io::BufferReader bufferReader(buffer);
    auto batchReader = (arrow::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();
    auto batch = batchReader->Next().ValueOrDie();
    return batch;
}

std::vector<i64> ReadIntegerArray(const std::shared_ptr<arrow::Array>& array)
{
    auto int64Array = std::dynamic_pointer_cast<arrow::Int64Array>(array);
    YT_VERIFY(int64Array);
    return {int64Array->raw_values(), int64Array->raw_values() + int64Array->length()};
}

std::vector<ui32> ReadInterger32Array(const std::shared_ptr<arrow::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow::UInt32Array>(array);
    YT_VERIFY(int32Array);
    return {int32Array->raw_values(), int32Array->raw_values() + int32Array->length()};
}

std::vector<std::string> ReadStringArray(const std::shared_ptr<arrow::Array>& array)
{
    auto arraySize = array->length();
    auto binArray = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
    YT_VERIFY(binArray);

    std::vector<std::string> stringArray;
    for (int i = 0; i < arraySize; i++) {
        stringArray.push_back(binArray->GetString(i));
    }
    return stringArray;
}

std::vector<std::string> ReadStringArrayFromDictionaryArray(const std::shared_ptr<arrow::Array>& array)
{
    auto dictArray = std::dynamic_pointer_cast<arrow::DictionaryArray>(array);
    YT_VERIFY(dictArray);

    auto indices = ReadInterger32Array(dictArray->indices());

    // Get values array.
    auto values =  ReadStringArray(dictArray->dictionary());

    std::vector<std::string> result;
    for (auto index : indices) {
        auto value = values[index];
        result.push_back(value);
    }
    return result;
}

std::vector<float> ReadFloatArray(const std::shared_ptr<arrow::Array>& array)
{
    auto floatArray = std::dynamic_pointer_cast<arrow::FloatArray>(array);
    YT_VERIFY(floatArray);
    return  {floatArray->raw_values(), floatArray->raw_values() + array->length()};
}

TEST_F(TArrowTestBase, YTADMINREQ_33599)
{
    TRichYPath tablePath("//tmp/test_arrow_reading");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"StringColumn", EValueType::String}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;
    const int rowCount = 20;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto columnId = writer->GetNameTable()->GetIdOrRegisterName("StringColumn");
        std::vector<TUnversionedRow> rows;
        std::vector<TUnversionedRowBuilder> rowsBuilders(rowCount);

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            rowsBuilders[rowIdx].AddValue(MakeUnversionedStringValue("VeryLongString", columnId));
        }

        for (int rowIdx = 0; rowIdx < std::ssize(rowsBuilders); rowIdx++) {
            rows.push_back(rowsBuilders[rowIdx].GetRow());
        }

        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
    req->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    req->set_format("<format=text>yson");

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

    NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    while (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        auto payloadRef = NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

        if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
            auto batch = MakeBatch(payloadRef.ToStringBuf());
            EXPECT_EQ(batch->num_columns(), 1);
            EXPECT_EQ(batch->column_name(0),"StringColumn");
            std::vector<std::string> expectedArray(rowCount, "VeryLongString");
            EXPECT_EQ(ReadStringArrayFromDictionaryArray(batch->column(0)), expectedArray);
        }
    }
}

TEST_F(TArrowTestBase, TestArrowReadingWithSystemColumns)
{
    WaitFor(Client_->SetNode("//sys/rpc_proxies/@config", ConvertToYsonString(THashMap<TString, int>{})))
        .ThrowOnError();

    WaitFor(Client_->SetNode(
        "//sys/rpc_proxies/@config/api",
        ConvertToYsonString(THashMap<TString, int>{{"read_buffer_row_count", 1}}))).ThrowOnError();

    Sleep(TDuration::Seconds(0.5));

    TRichYPath tablePath("//tmp/test_arrow_reading_with_system_columns");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"IntColumn", EValueType::Int64}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();

        auto intColumnId = writer->GetNameTable()->GetIdOrRegisterName("IntColumn");

        int chunkRowCount = 3;
        for (int chunkIdx = 0; chunkIdx < 2; ++chunkIdx) {
            std::vector<TUnversionedRow> rows;
            rows.reserve(chunkRowCount);
            std::vector<TUnversionedRowBuilder> rowsBuilders(chunkRowCount);

            for (int rowIdx = 0; rowIdx < chunkRowCount; rowIdx++) {
                rowsBuilders[rowIdx].AddValue(MakeUnversionedInt64Value(chunkIdx * chunkRowCount + rowIdx, intColumnId));
            }

            for (int rowIdx = 0; rowIdx < std::ssize(rowsBuilders); rowIdx++) {
                rows.push_back(rowsBuilders[rowIdx].GetRow());
            }

            YT_VERIFY(writer->Write(rows));
        }
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
    req->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    req->set_format("<format=text>yson");

    // Ask range_index and row_index column.
    req->set_enable_range_index(true);
    req->set_enable_row_index(true);

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

    NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    if (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        auto payloadRef = NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

        if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
            auto batch = MakeBatch(payloadRef.ToStringBuf());
            EXPECT_EQ(batch->num_columns(), 3);
        }
    }
}

TEST_F(TArrowTestBase, TestArrowReadingWithoutSystemColumns)
{
    TRichYPath tablePath("//tmp/test_arrow_reading_without_system_columns");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"IntColumn", EValueType::Int64}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto columnId = writer->GetNameTable()->GetIdOrRegisterName("IntColumn");

        auto value = MakeUnversionedInt64Value(1, columnId);
        TUnversionedOwningRow owningRow(MakeRange(&value, 1));

        YT_VERIFY(writer->Write({owningRow}));
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
    req->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    req->set_format("<format=text>yson");

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

    NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    while (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        auto payloadRef = NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

        if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
            auto batch = MakeBatch(payloadRef.ToStringBuf());
            EXPECT_EQ(batch->num_columns(), 1);
            EXPECT_EQ(batch->column_name(0),"IntColumn");
            std::vector<i64> expectedArray(1, 1);
            EXPECT_EQ(ReadIntegerArray(batch->column(0)), expectedArray);
        }
    }
}

TEST_F(TArrowTestBase, TestArrowNullColumns)
{
    TRichYPath tablePath("//tmp/test_arrow_reading_null_columns");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"NullColumn", ESimpleLogicalValueType::Null}, {"VoidColumn", ESimpleLogicalValueType::Void}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
        auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto nullColumnId = writer->GetNameTable()->GetIdOrRegisterName("NullColumn");
        auto nullValue = MakeUnversionedNullValue(nullColumnId);

        auto voidColumnId = writer->GetNameTable()->GetIdOrRegisterName("VoidColumn");
        auto voidValue = MakeUnversionedNullValue(voidColumnId);

        TUnversionedRowBuilder rowBuilder;
        rowBuilder.AddValue(nullValue);
        rowBuilder.AddValue(voidValue);

        YT_VERIFY(writer->Write({rowBuilder.GetRow()}));
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
    req->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    req->set_format("<format=text>yson");

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

    NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    while (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        auto payloadRef = NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

        if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
            auto batch = MakeBatch(payloadRef.ToStringBuf());
            EXPECT_EQ(batch->num_columns(), 2);
            EXPECT_EQ(batch->column_name(0),"NullColumn");
            EXPECT_TRUE(std::dynamic_pointer_cast<arrow::NullArray>(batch->column(0)));

            EXPECT_EQ(batch->column_name(1),"VoidColumn");
            EXPECT_TRUE(std::dynamic_pointer_cast<arrow::NullArray>(batch->column(1)));
        }
    }
}

TEST_F(TArrowTestBase, Float)
{
    TRichYPath tablePath("//tmp/test_arrow_reading");
    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"FloatColumn", ESimpleLogicalValueType::Float}}));
    options.Attributes->Set("optimize_for", "scan");
    options.Force = true;

    WaitFor(Client_->CreateNode(tablePath.GetPath(), EObjectType::Table, options))
        .ThrowOnError();

    {
         auto writer = WaitFor(Client_->CreateTableWriter(tablePath))
            .ValueOrThrow();
        auto floatColumnId = writer->GetNameTable()->GetIdOrRegisterName("FloatColumn");
        auto floatValue = MakeUnversionedDoubleValue(3.14, floatColumnId);

        TUnversionedRowBuilder rowBuilder;
        rowBuilder.AddValue(floatValue);

        YT_VERIFY(writer->Write({rowBuilder.GetRow()}));
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    auto apiServiceProxy = VerifyDynamicCast<NYT::NApi::NRpcProxy::TClientBase*>(Client_.Get())->CreateApiServiceProxy();
    auto req = apiServiceProxy.ReadTable();

    req->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
    req->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
    req->set_format("<format=text>yson");

    ToProto(req->mutable_path(), tablePath);
    auto stream = WaitFor(NRpc::CreateRpcClientInputStream(req))
        .ValueOrThrow();

    auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

    NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    while (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        auto payloadRef = NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

        if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
            auto batch = MakeBatch(payloadRef.ToStringBuf());
            EXPECT_EQ(batch->num_columns(), 1);
            EXPECT_EQ(batch->column_name(0),"FloatColumn");
            EXPECT_EQ(batch->num_columns(), 1);

            std::vector<float> expectedArray(1, 3.14);
            EXPECT_EQ(ReadFloatArray(batch->column(0)), expectedArray);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
