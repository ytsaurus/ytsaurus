//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0

#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/rowset.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <cstdlib>
#include <functional>
#include <tuple>

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TString& str, ::std::ostream* os)
{
    *os << str;
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TApiTestBase
    : public ::testing::Test
{
protected:
    static INativeConnectionPtr Connection_;
    static INativeClientPtr Client_;

    static void SetUpTestCase()
    {
        const auto* configPath = std::getenv("YT_CONSOLE_DRIVER_CONFIG_PATH");
        TIFStream configStream(configPath);
        auto config = ConvertToNode(&configStream)->AsMap();

        if (auto logging = config->FindChild("logging")) {
            NLogging::TLogManager::Get()->Configure(ConvertTo<NLogging::TLogConfigPtr>(logging));
        }

        Connection_ = CreateNativeConnection(ConvertTo<TNativeConnectionConfigPtr>(config->GetChild("driver")));

        TClientOptions clientOptions;
        clientOptions.User = "root";
        Client_ = Connection_->CreateNativeClient(clientOptions);
    }

    static void TearDownTestCase()
    {
        Client_.Reset();
        Connection_.Reset();
    }
};

INativeConnectionPtr TApiTestBase::Connection_;
INativeClientPtr TApiTestBase::Client_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TApiTestBase, TestClusterConnection)
{
    auto resOrError = Client_->GetNode(TYPath("/"));
    EXPECT_TRUE(resOrError.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TDynamicTablesTestBase
    : public TApiTestBase
{
protected:
    static void SetUpTestCase()
    {
        TApiTestBase::SetUpTestCase();

        auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
            .ValueOrThrow();
        WaitUntil(TYPath("#") + ToString(cellId) + "/@health", "good");

        WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(1000)))
            .ThrowOnError();
    }

    static void TearDownTestCase()
    {
        RemoveSystemObjects("//sys/tablet_cells");
        RemoveSystemObjects("//sys/tablet_cell_bundles", [] (const TString& name) {
            return name != "default";
        });

        WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(0)))
            .ThrowOnError();

        TApiTestBase::TearDownTestCase();
    }

    static void SyncMountTable(const TYPath& path)
    {
        WaitFor(Client_->MountTable(path))
            .ThrowOnError();
        WaitUntil(path + "/@tablet_state", "mounted");
    }

    static void SyncUnmountTable(const TYPath& path)
    {
        WaitFor(Client_->UnmountTable(path))
            .ThrowOnError();
        WaitUntil(path + "/@tablet_state", "unmounted");
    }

    static void WaitUntil(const TYPath& path, const TString& expected)
    {
        auto start = Now();
        bool reached = false;
        for (int attempt = 0; attempt < 2*30; ++attempt) {
            auto state = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            auto value = ConvertTo<IStringNodePtr>(state)->GetValue();
            if (value == expected) {
                reached = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(500));
        }

        if (!reached) {
            THROW_ERROR_EXCEPTION("%Qv is not %Qv after %v seconds",
                path,
                expected,
                (Now() - start).Seconds());
        }
    }

private:
    static void RemoveSystemObjects(
        const TYPath& path,
        std::function<bool(const TString&)> filter = [] (const TString&) { return true; })
    {
        auto items = WaitFor(Client_->ListNode(path))
            .ValueOrThrow();
        auto itemsList = ConvertTo<IListNodePtr>(items);

        std::vector<TFuture<void>> asyncWait;
        for (const auto& item : itemsList->GetChildren()) {
            const auto& name = item->AsString()->GetValue();
            if (filter(name)) {
                asyncWait.push_back(Client_->RemoveNode(path + "/" + name));
            }
        }

        WaitFor(Combine(asyncWait))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

using TLookupFilterTestParam = std::tuple<
    std::vector<TString>,
    TString,
    SmallVector<int, TypicalColumnCount>,
    TString,
    TString,
    TString>;

class TLookupFilterTest
    : public TDynamicTablesTestBase
    , public ::testing::WithParamInterface<TLookupFilterTestParam>
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        Table_ = TYPath("//tmp/lookup_test");
        auto attributes = ConvertToNode(TYsonString(
            "{dynamic=%true;schema=["
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]}"));

        TCreateNodeOptions options;
        options.Attributes = ConvertToAttributes(attributes);

        WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
            .ThrowOnError();

        SyncMountTable(Table_);

        InitializeRows();
    }

    static void TearDownTestCase()
    {
        SyncUnmountTable(Table_);

        WaitFor(Client_->RemoveNode(TYPath("//tmp/*")))
            .ThrowOnError();

        TDynamicTablesTestBase::TearDownTestCase();
    }

protected:
    static TYPath Table_;
    static THashMap<int, TTimestamp> CommitTimestamps_;
    TRowBufferPtr Buffer_ = New<TRowBuffer>();

    static void InitializeRows()
    {
        WriteUnversionedRow(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12; <id=3> 13; <id=4> 14; <id=5> 15",
            0);
    }

    static void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString,
        int transactionTag)
    {
        auto preparedRow = PrepareUnversionedRow(names, rowString);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow),
            transactionTag);
    }

    static void WriteRows(
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows,
        int timestampTag)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();

        const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
        ASSERT_EQ(timestamps.size(), 1);
        CommitTimestamps_[timestampTag] = timestamps[0].second;
    }

    static std::tuple<TSharedRange<TUnversionedRow>, TNameTablePtr> PrepareUnversionedRow(
        const std::vector<TString>& names,
        const TString& rowString)
    {
        auto nameTable = New<TNameTable>();
        for (const auto& name : names) {
            nameTable->GetIdOrRegisterName(name);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto owningRow = YsonToSchemalessRow(rowString);
        std::vector<TUnversionedRow> rows{rowBuffer->Capture(owningRow.Get())};
        return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson)
    {
        auto immutableRow = YsonToVersionedRow(Buffer_, keyYson, valueYson);
        auto row = TMutableVersionedRow(const_cast<TVersionedRowHeader*>(immutableRow.GetHeader()));

        for (auto* value = row.BeginValues(); value < row.EndValues(); ++value) {
            value->Timestamp = CommitTimestamps_[value->Timestamp];
        }
        for (auto* timestamp = row.BeginWriteTimestamps(); timestamp < row.EndWriteTimestamps(); ++timestamp) {
            *timestamp = CommitTimestamps_[*timestamp];
        }

        return row;
    }
};

TYPath TLookupFilterTest::Table_;
THashMap<int, TTimestamp> TLookupFilterTest::CommitTimestamps_;

////////////////////////////////////////////////////////////////////////////////

static auto s = TString("<unique_keys=%true;strict=%true>");
static auto su = TString("<unique_keys=%false;strict=%true>");
static auto k0 = "{name=k0;type=int64;sort_order=ascending};";
static auto k1 = "{name=k1;type=int64;sort_order=ascending};";
static auto k2 = "{name=k2;type=int64;sort_order=ascending};";
static auto ku0 = "{name=k0;type=int64};";
static auto ku1 = "{name=k1;type=int64};";
static auto ku2 = "{name=k2;type=int64};";
static auto v3 = "{name=v3;type=int64};";
static auto v4 = "{name=v4;type=int64};";
static auto v5 = "{name=v5;type=int64};";

TEST_F(TLookupFilterTest, TestLookupAll)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 10; <id=1> 11; <id=2> 12");

    auto res = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow("<id=0> 10; <id=1> 11; <id=2> 12; <id=3> 13; <id=4> 14; <id=5> 15"));
    EXPECT_EQ(actual, expected);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(actualSchema, expectedSchema);
}

TEST_F(TLookupFilterTest, TestVersionedLookupAll)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 10; <id=1> 11; <id=2> 12");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 10; <id=1> 11; <id=2> 12",
        "<id=3;ts=0> 13; <id=4;ts=0> 14; <id=5;ts=0> 15"));
    EXPECT_EQ(actual, expected);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(actualSchema, expectedSchema);
}

TEST_P(TLookupFilterTest, TestLookupFilter)
{
    const auto& param = GetParam();
    const auto& namedColumns = std::get<0>(param);
    const auto& keyString = std::get<1>(param);
    const auto& columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);
    auto rowString = resultKeyString + resultValueString;

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TLookupRowsOptions options;
    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = columnFilter;

    auto res = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow(rowString));
    EXPECT_EQ(actual, expected)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRow: " << rowString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(actualSchema, expectedSchema)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRow: " << rowString << std::endl
        << "expectedSchema: " << schemaString << std::endl;
}

TEST_P(TLookupFilterTest, TestVersionedLookupFilter)
{
    const auto& param = GetParam();
    const auto& namedColumns = std::get<0>(param);
    const auto& keyString = std::get<1>(param);
    const auto& columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TVersionedLookupRowsOptions options;
    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = columnFilter;

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(resultKeyString, resultValueString));
    EXPECT_EQ(actual, expected)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(actualSchema, expectedSchema)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;
}

INSTANTIATE_TEST_CASE_P(
    TLookupFilterTest,
    TLookupFilterTest,
    ::testing::Values(
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,1,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            s + "[" + k0 + k1 + k2 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,2,1},
            "<id=0> 10; <id=1> 12; <id=2> 11;", "",
            su + "[" + k0 + ku2 + ku1 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {1,0,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            s + "[" + k0 + k1 + k2 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {3,4,5},
            "", "<id=0;ts=0> 13; <id=1;ts=0> 14; <id=2;ts=0> 15;",
            su + "[" + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {1,5,3},
            "<id=0> 11;", "<id=1;ts=0> 15; <id=2;ts=0> 13;",
            su + "[" + ku1 + v5 + v3 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {3,4,5},
            "", "<id=0;ts=0> 13; <id=1;ts=0> 14; <id=2;ts=0> 15;",
            su + "[" + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {5,3,4},
            "", "<id=0;ts=0> 15; <id=1;ts=0> 13; <id=2;ts=0> 14;",
            su + "[" + v5 + v3 + v4 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2", "v5", "v3", "v4"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {1,0,2,4,5,3},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "<id=3;ts=0> 13; <id=4;ts=0> 14; <id=5;ts=0> 15;",
            s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2", "v5", "v3", "v4"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {2,1,5,4},
            "<id=0> 12; <id=1> 10;", "<id=2;ts=0> 14; <id=3;ts=0> 13;",
            su + "[" + ku2 + ku0 + v4 + v3 + "]")
));

TEST_F(TLookupFilterTest, TestRetentionConfig)
{
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 20; <id=1> 20; <id=2> 20; <id=3> 20;",
        1);
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 20; <id=1> 20; <id=2> 20; <id=3> 21;",
        2);

    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v4"},
        "<id=0> 20; <id=1> 20; <id=2> 20");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21; <id=3;ts=1> 20;"));
    EXPECT_EQ(actual, expected);

    TVersionedLookupRowsOptions options;
    options.RetentionConfig = New<TRetentionConfig>();
    options.RetentionConfig->MinDataTtl = TDuration::MilliSeconds(0);
    options.RetentionConfig->MaxDataTtl = TDuration::MilliSeconds(1800000);
    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;
    options.Timestamp = CommitTimestamps_[2] + 1;

    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21;"));
    EXPECT_EQ(actual, expected);

    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = SmallVector<int, TypicalColumnCount>{0,1,2,3};

    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        ""));
    EXPECT_EQ(actual, expected);

    options.ColumnFilter.Indexes = SmallVector<int, TypicalColumnCount>{3};

    preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 20; <id=1> 20; <id=2> 20");
    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "",
        "<id=0;ts=2> 21;"));
    EXPECT_EQ(actual, expected);

}

////////////////////////////////////////////////////////////////////////////////

class TVersionedWriteTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        TClientOptions clientOptions;
        clientOptions.User = "replicator";
        Client_ = Connection_->CreateNativeClient(clientOptions);

        Table_ = TYPath("//tmp/write_test");
        auto attributes = ConvertToNode(TYsonString(
            "{dynamic=%true;schema=["
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]}"));

        TCreateNodeOptions options;
        options.Attributes = ConvertToAttributes(attributes);

        WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
            .ThrowOnError();

        SyncMountTable(Table_);
    }

    static void TearDownTestCase()
    {
        SyncUnmountTable(Table_);
        Client_.Reset();
        TDynamicTablesTestBase::TearDownTestCase();
    }

protected:
    static INativeClientPtr Client_;
    static TYPath Table_;
    TRowBufferPtr Buffer_ = New<TRowBuffer>();

    static void WriteVersionedRow(
        std::vector<TString> names,
        const TString& keyYson,
        const TString& valueYson)
    {
        auto preparedRow = PrepareVersionedRow(names, keyYson, valueYson);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow));
    }

    static void WriteRows(
        TNameTablePtr nameTable,
        TSharedRange<TVersionedRow> rows)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();
    }

    static std::tuple<TSharedRange<TVersionedRow>, TNameTablePtr> PrepareVersionedRow(
        const std::vector<TString>& names,
        const TString& keyYson,
        const TString& valueYson)
    {
        auto nameTable = New<TNameTable>();
        for (const auto& name : names) {
            nameTable->GetIdOrRegisterName(name);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto row = YsonToVersionedRow(rowBuffer, keyYson, valueYson);
        std::vector<TVersionedRow> rows{row};
        return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    static std::tuple<TSharedRange<TUnversionedRow>, TNameTablePtr> PrepareUnversionedRow(
        const std::vector<TString>& names,
        const TString& rowString)
    {
        auto nameTable = New<TNameTable>();
        for (const auto& name : names) {
            nameTable->GetIdOrRegisterName(name);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto owningRow = YsonToSchemalessRow(rowString);
        std::vector<TUnversionedRow> rows{rowBuffer->Capture(owningRow.Get())};
        return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson)
    {
        return YsonToVersionedRow(Buffer_, keyYson, valueYson);
    }
};

INativeClientPtr TVersionedWriteTest::Client_;
TYPath TVersionedWriteTest::Table_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedWriteTest, TestWriteRemapping)
{
    WriteVersionedRow(
        {"k0", "k1", "k2", "v5", "v3", "v4"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=1> 15; <id=4;ts=1> 13; <id=5;ts=1> 14;");
    WriteVersionedRow(
        {"k0", "k1", "k2", "v4", "v5", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=2> 24; <id=4;ts=2> 25; <id=5;ts=2> 23;");

    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    ASSERT_EQ(res->GetRows().Size(), 1);

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=2> 23; <id=3;ts=1> 13; <id=4;ts=2> 24; <id=4;ts=1> 14; <id=5;ts=2> 25; <id=5;ts=1> 15;"));
    EXPECT_EQ(actual, expected);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k2", "k1", "v3"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=3> 100;"),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "v3", "k2"},
            "<id=0> 30; <id=1> 30; <id=3> 30;",
            "<id=2;ts=3> 100;"),
        TErrorException);
}

TEST_F(TVersionedWriteTest, TestWriteTypeChecking)
{
    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=2;ts=3> %true;"),
        TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
