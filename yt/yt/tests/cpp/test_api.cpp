#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/public.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/queue_client/queue_rowset.h>
#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <tuple>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NQueueClient;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TApiTestBase, TestClusterConnection)
{
    auto resOrError = Client_->GetNode(TYPath("/"));
    EXPECT_TRUE(resOrError.Get().IsOK());
}

TEST_F(TApiTestBase, TestCreateInvalidNode)
{
    auto resOrError = Client_->CreateNode(TYPath("//tmp/a"), EObjectType::SortedDynamicTabletStore);
    EXPECT_FALSE(resOrError.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

using TLookupFilterTestParam = std::tuple<
    std::vector<TString>,
    TString,
    std::vector<int>,
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

        CreateTable(
            "//tmp/lookup_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]");

        InitializeRows();
    }

protected:
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
        int timestampTag)
    {
        auto preparedRow = PrepareUnversionedRow(names, rowString);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow),
            timestampTag);
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
        ASSERT_EQ(1u, timestamps.size());
        CommitTimestamps_[timestampTag] = timestamps[0].second;
    }

    static void DeleteRow(
        std::vector<TString> names,
        const TString& rowString,
        int timestampTag)
    {
        auto preparedKey = PrepareUnversionedRow(names, rowString);
        DeleteRows(
            std::get<1>(preparedKey),
            std::get<0>(preparedKey),
            timestampTag);
    }

    static void DeleteRows(
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows,
        int timestampTag)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->DeleteRows(Table_, nameTable, rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();

        const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
        ASSERT_EQ(1u, timestamps.size());
        CommitTimestamps_[timestampTag] = timestamps[0].second;
    }

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson,
        const std::vector<TTimestamp>& extraWriteTimestamps = {},
        const std::vector<TTimestamp>& deleteTimestamps = {})
    {
        auto immutableRow = YsonToVersionedRow(
            Buffer_,
            keyYson,
            valueYson,
            deleteTimestamps,
            extraWriteTimestamps);
        auto row = TMutableVersionedRow(const_cast<TVersionedRowHeader*>(immutableRow.GetHeader()));

        for (auto& value : row.Values()) {
            value.Timestamp = GetOrCrash(CommitTimestamps_, value.Timestamp);
        }
        for (auto& timestamp : row.WriteTimestamps()) {
            timestamp = GetOrCrash(CommitTimestamps_, timestamp);
        }
        for (auto& timestamp : row.DeleteTimestamps()) {
            timestamp = GetOrCrash(CommitTimestamps_, timestamp);
        }

        return row;
    }
};

THashMap<int, TTimestamp> TLookupFilterTest::CommitTimestamps_;

////////////////////////////////////////////////////////////////////////////////

static auto su = TString("<unique_keys=%false;strict=%true>");
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

    auto rowset = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow()
        .Rowset;

    auto actual = ToString(rowset->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow("<id=0> 10; <id=1> 11; <id=2> 12; <id=3> 13; <id=4> 14; <id=5> 15"));
    EXPECT_EQ(expected, actual);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        su + "[" + ku0 + ku1 + ku2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(rowset->GetSchema(), EYsonFormat::Text).ToString();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).ToString();
    EXPECT_EQ(expectedSchema, actualSchema);
}

TEST_F(TLookupFilterTest, TestVersionedLookupAll)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 10; <id=1> 11; <id=2> 12");

    auto rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow()
        .Rowset;

    auto actual = ToString(rowset->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 10; <id=1> 11; <id=2> 12",
        "<id=3;ts=0> 13; <id=4;ts=0> 14; <id=5;ts=0> 15"));
    EXPECT_EQ(expected, actual);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        su + "[" + ku0 + ku1 + ku2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(rowset->GetSchema(), EYsonFormat::Text).ToString();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).ToString();
    EXPECT_EQ(expectedSchema, actualSchema);
}

TEST_P(TLookupFilterTest, TestLookupFilter)
{
    const auto& param = GetParam();
    const auto& namedColumns = std::get<0>(param);
    const auto& keyString = std::get<1>(param);
    auto columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);
    auto rowString = resultKeyString + resultValueString;

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TLookupRowsOptions options;
    options.ColumnFilter = TColumnFilter(columnFilter);

    auto rowset = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    auto actual = ToString(rowset->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow(rowString));
    EXPECT_EQ(expected, actual)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRow: " << rowString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(rowset->GetSchema(), EYsonFormat::Text).ToString();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).ToString();
    EXPECT_EQ(expectedSchema, actualSchema)
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
    auto columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);

    bool hasNonKeyColumns = false;
    for (const auto& column : namedColumns) {
        if (column.StartsWith("v")) {
            hasNonKeyColumns = true;
        }
    }

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TVersionedLookupRowsOptions options;
    options.ColumnFilter = TColumnFilter(columnFilter);

    auto rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    auto actual = ToString(rowset->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        resultKeyString,
        resultValueString,
        hasNonKeyColumns ? std::vector<TTimestamp>{} : std::vector<TTimestamp>{0}));
    EXPECT_EQ(expected, actual)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(rowset->GetSchema(), EYsonFormat::Text).ToString();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).ToString();
    EXPECT_EQ(expectedSchema, actualSchema)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TLookupFilterTest,
    TLookupFilterTest,
    ::testing::Values(
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,1,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            su + "[" + ku0 + ku1 + ku2 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,2,1},
            "<id=0> 10; <id=1> 12; <id=2> 11;", "",
            su + "[" + ku0 + ku2 + ku1 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {1,0,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            su + "[" + ku0 + ku1 + ku2 + "]"),
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
            su + "[" + ku0 + ku1 + ku2 + v3 + v4 + v5 + "]"),
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

    auto rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    auto actual = ToString(rowset->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21; <id=3;ts=1> 20;"));
    EXPECT_EQ(expected, actual);

    TVersionedLookupRowsOptions options;
    options.RetentionConfig = New<TRetentionConfig>();
    options.RetentionConfig->MinDataTtl = TDuration::MilliSeconds(0);
    options.RetentionConfig->MaxDataTtl = TDuration::MilliSeconds(1800000);
    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;
    options.Timestamp = CommitTimestamps_[2] + 1;

    rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    actual = ToString(rowset->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21;"));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter = TColumnFilter({0,1,2,3});

    rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    actual = ToString(rowset->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "",
        {2}));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter = TColumnFilter({3});

    preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 20; <id=1> 20; <id=2> 20");
    rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    actual = ToString(rowset->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "",
        "<id=0;ts=2> 21;"));
    EXPECT_EQ(expected, actual);
}

// YT-7668
// Checks that in cases like
//   insert(key=k, value1=x, value2=y)
//   delete(key=k)
//   insert(key=k, value1=x)
//   versioned_lookup(key=k, column_filter=[value1])
// the information about the presence of the second insertion is not lost,
// although no versioned values are returned.
TEST_F(TLookupFilterTest, TestFilteredOutTimestamps)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30");
    TVersionedLookupRowsOptions options;

    auto executeLookup = [&] {
        auto rowset = WaitFor(Client_->VersionedLookupRows(
            Table_,
            std::get<1>(preparedKey),
            std::get<0>(preparedKey),
            options))
            .ValueOrThrow()
            .Rowset;
        EXPECT_EQ(1u, rowset->GetRows().Size());
        return ToString(rowset->GetRows()[0]);
    };

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 1; <id=4> 1; <id=5> 1",
        1);

    DeleteRows(std::get<1>(preparedKey), std::get<0>(preparedKey), 2);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 3;",
        3);

    options.ColumnFilter = TColumnFilter();
    options.RetentionConfig = New<TRetentionConfig>();
    options.RetentionConfig->MinDataTtl = TDuration::MilliSeconds(0);
    options.RetentionConfig->MaxDataTtl = TDuration::MilliSeconds(1800000);
    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;

    auto actual = executeLookup();
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=3> 3",
        {},
        {2}));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter = TColumnFilter({0, 1, 2, 4});

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "",
        {3},
        {2}));
    EXPECT_EQ(expected, actual);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v4"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 4",
        4);

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=4> 4",
        {3},
        {2}));
    EXPECT_EQ(expected, actual);

    DeleteRows(std::get<1>(preparedKey), std::get<0>(preparedKey), 5);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 6;",
        6);

    options.ColumnFilter = TColumnFilter({0, 1, 2, 4, 5});
    options.RetentionConfig->MinDataVersions = 2;
    options.RetentionConfig->MaxDataVersions = 2;

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=4> 4",
        {6},
        {2, 5}));
    EXPECT_EQ(expected, actual);

    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "",
        {6},
        {2, 5}));
    EXPECT_EQ(expected, actual);
}

TEST_F(TLookupFilterTest, TestLookupDuplicateKeyColumns)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 20; <id=1> 21; <id=2> 22; <id=2> 22");

    EXPECT_THROW(WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
            .ValueOrThrow(), TErrorException);
}

TEST_F(TLookupFilterTest, YT_10159)
{
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 1; <id=1> 1; <id=2> 1; <id=3> 1",
        7);
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 99; <id=1> 99; <id=2> 99; <id=3> 22",
        8);

    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 99; <id=1> 99; <id=2> 99");

    for (int iter = 0; iter < 2; ++iter) {
        TVersionedLookupRowsOptions options;

        {
            options.Timestamp = CommitTimestamps_[7];
            auto rowset = WaitFor(Client_->VersionedLookupRows(
                Table_,
                std::get<1>(preparedKey),
                std::get<0>(preparedKey),
                options))
                .ValueOrThrow()
                .Rowset;
            EXPECT_EQ(0u, rowset->GetRows().Size());
        }

        {
            options.Timestamp = CommitTimestamps_[8];
            auto rowset = WaitFor(Client_->VersionedLookupRows(
                Table_,
                std::get<1>(preparedKey),
                std::get<0>(preparedKey),
                options))
                .ValueOrThrow()
                .Rowset;
            EXPECT_EQ(1u, rowset->GetRows().Size());
        }

        if (iter == 0) {
            SyncUnmountTable(Table_);
            SyncMountTable(Table_);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicTablesTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        CreateTable(
            "//tmp/write_ordered_test", // tablePath
            "[" // schema
            "{name=v1;type=int64};"
            "{name=v2;type=int64};"
            "{name=v3;type=int64}]");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedDynamicTablesTest, TestOrderedTableWrite)
{
    WriteUnversionedRow(
        {"v3", "v1", "v2"},
        "<id=0> 15; <id=1> 13; <id=2> 14;");
    WriteUnversionedRow(
        {"v2", "v3", "v1"},
        "<id=0> 24; <id=1> 25; <id=2> 23;");

    WriteUnversionedRow(
        {"v3", "v1", "v2", "$tablet_index"},
        "<id=0> 15; <id=1> 13; <id=2> 14; <id=3> #;");
    WriteUnversionedRow(
        {"v2", "v3", "v1", "$tablet_index"},
        "<id=0> 24; <id=1> 25; <id=2> 23; <id=3> 0;");

    auto rowset = WaitFor(Client_->SelectRows(Format("* from [%v]", Table_)))
        .ValueOrThrow()
        .Rowset;
    auto rows = rowset->GetRows();

    ASSERT_EQ(4u, rows.Size());

    auto actual = ToString(rows[0]);
    auto expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 0; <id=2> 13; <id=3> 14; <id=4> 15;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[1]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 1; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[2]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 2; <id=2> 13; <id=3> 14; <id=4> 15;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[3]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 3; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

class TQueueApiTest
    : public TOrderedDynamicTablesTest, public testing::WithParamInterface<bool>
{
public:
    static void WaitForRowCount(i64 rowCount)
    {
        WaitForPredicate([rowCount] {
            auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", Table_)))
                .ValueOrThrow();

            return std::ssize(allRowsResult.Rowset->GetRows()) == rowCount;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TQueueApiTest, TestQueueApi)
{
    CreateTable(
        Format("//tmp/test_queue_api_%v", GetParam()), // tablePath
        "[" // schema
        "{name=v1;type=int64};"
        "{name=v2;type=int64};"
        "{name=v3;type=int64}]");

    TPullQueueOptions pullQueueOptions;
    pullQueueOptions.UseNativeTabletNodeApi = GetParam();

    WriteUnversionedRow(
        {"v3", "v1", "v2"},
        "<id=0> 15; <id=1> 13; <id=2> 14;");
    WriteUnversionedRow(
        {"v2", "v3", "v1"},
        "<id=0> 24; <id=1> 25; <id=2> 23;");

    // Remount table to cause stores to flush.
    SyncUnmountTable(Table_);
    SyncMountTable(Table_);

    auto options = TQueueRowBatchReadOptions{.MaxRowCount = 1};
    auto res = WaitFor(Client_->PullQueue(Table_, 0, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 0);
    EXPECT_EQ(res->GetFinishOffset(), 1);
    auto rows = res->GetRows();
    ASSERT_EQ(rows.Size(), 1u);

    auto actual = ToString(rows[0]);
    auto expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 0; <id=2> 13; <id=3> 14; <id=4> 15;"));
    EXPECT_EQ(expected, actual);

    WriteUnversionedRow(
        {"v1", "v2", "v3"},
        "<id=0> 123; <id=1> 124; <id=2> 125;");

    options = TQueueRowBatchReadOptions{.MaxRowCount = 10};
    res = WaitFor(Client_->PullQueue(Table_, 1, 0, options))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 1);
    EXPECT_LE(res->GetFinishOffset(), 3);
    rows = res->GetRows();
    ASSERT_GE(rows.size(), 1u);
    EXPECT_LE(rows.size(), 2u);

    actual = ToString(rows[0]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 1; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);

    YT_UNUSED_FUTURE(Client_->TrimTable(Table_, 0, 1));
    WaitForRowCount(2);

    WriteUnversionedRow(
        {"v1", "v2", "v3"},
        "<id=0> 1123; <id=1> 1124; <id=2> 1125;");

    options = TQueueRowBatchReadOptions{.MaxRowCount = 2};
    res = WaitFor(Client_->PullQueue(Table_, 0, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 1);
    EXPECT_LE(res->GetFinishOffset(), 3);
    rows = res->GetRows();
    ASSERT_GE(rows.size(), 1u);
    EXPECT_LE(rows.size(), 2u);

    actual = ToString(rows[0]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 1; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);

    YT_UNUSED_FUTURE(Client_->TrimTable(Table_, 0, 2));
    WaitForRowCount(2);

    options = TQueueRowBatchReadOptions{.MaxRowCount = 2};
    res = WaitFor(Client_->PullQueue(Table_, 0, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 2);
    EXPECT_LE(res->GetFinishOffset(), 4);
    rows = res->GetRows();
    ASSERT_GE(rows.size(), 1u);
    EXPECT_LE(rows.size(), 2u);

    actual = ToString(rows[0]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 2; <id=2> 123; <id=3> 124; <id=4> 125;"));
    EXPECT_EQ(expected, actual);

    options = TQueueRowBatchReadOptions{.MaxRowCount = 2};
    res = WaitFor(Client_->PullQueue(Table_, 10, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 10);
    EXPECT_EQ(res->GetFinishOffset(), 10);
    rows = res->GetRows();
    ASSERT_EQ(rows.size(), 0u);

    options = TQueueRowBatchReadOptions{.MaxRowCount = 10, .MaxDataWeight = 5, .DataWeightPerRowHint = 3};
    res = WaitFor(Client_->PullQueue(Table_, 0, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 2);
    EXPECT_EQ(res->GetFinishOffset(), 3);
    rows = res->GetRows();
    ASSERT_EQ(rows.size(), 1u);

    actual = ToString(rows[0]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 2; <id=2> 123; <id=3> 124; <id=4> 125;"));
    EXPECT_EQ(expected, actual);

    // TODO(achulkov2): Add test with trimming and several chunks.

}

TEST_P(TQueueApiTest, PullQueueCanReadBigBatches)
{
    CreateTable(
        Format("//tmp/pull_queue_can_read_big_batches_%v", GetParam()), // tablePath
        "[" // schema
        "{name=v1;type=string}]",
        /*mount*/ false);

    auto chunkWriterConfig = New<TChunkWriterConfig>();
    chunkWriterConfig->BlockSize = 1024;
    WaitFor(Client_->SetNode(Table_ + "/@chunk_writer", ConvertToYsonString(chunkWriterConfig)))
        .ThrowOnError();
    WaitFor(Client_->SetNode(Table_ + "/@compression_codec", ConvertToYsonString("none")))
        .ThrowOnError();
    WaitFor(Client_->SetNode(Table_ + "/@dynamic_store_auto_flush_period", ConvertToYsonString(GetEphemeralNodeFactory()->CreateEntity())))
        .ThrowOnError();

    SyncMountTable(Table_);

    TString bigString(1_MB, 'a');

    for (int i = 0; i < 20; ++i) {
        WriteUnversionedRow(
            {"v1"},
            Format("<id=0> %v;", bigString));
    }

    // Flush.
    SyncFreezeTable(Table_);
    SyncUnfreezeTable(Table_);

    auto chunkIds = ConvertTo<std::vector<NChunkClient::TChunkId>>(
        WaitFor(Client_->GetNode(Table_ + "/@chunk_ids")).ValueOrThrow());
    ASSERT_GE(chunkIds.size(), 1u);
    auto chunkId = chunkIds[0];

    auto compressedDataSize = ConvertTo<ui64>(WaitFor(Client_->GetNode(Format("#%v/@compressed_data_size", chunkId))).ValueOrThrow());
    auto maxBlockSize = ConvertTo<ui64>(WaitFor(Client_->GetNode(Format("#%v/@max_block_size", chunkId))).ValueOrThrow());

    ASSERT_GE(compressedDataSize, 10_MB);
    ASSERT_LE(maxBlockSize, 2_MB);

    TPullQueueOptions pullQueueOptions;
    pullQueueOptions.UseNativeTabletNodeApi = GetParam();

    auto options = TQueueRowBatchReadOptions{
        .MaxRowCount = 10000,
        .MaxDataWeight = 16_MB,
    };
    auto res = WaitFor(Client_->PullQueue(Table_, 0, 0, options, pullQueueOptions))
        .ValueOrThrow();
    EXPECT_EQ(res->GetStartOffset(), 0);
    auto rows = res->GetRows();
    // This is at least 10MB.
    ASSERT_GE(rows.Size(), 10u);
}

INSTANTIATE_TEST_SUITE_P(
    UseNativeTabletNodeApi,
    TQueueApiTest,
    testing::Values(false, true));

////////////////////////////////////////////////////////////////////////////////

class TClusterIdentificationTest
    : public TApiTestBase
{ };

TEST_F(TClusterIdentificationTest, FetchFromMaster)
{
    auto clusterName = Client_->GetClusterName();
    ASSERT_TRUE(clusterName);
    ASSERT_EQ(*clusterName, ClusterName_);

    {
        TForbidContextSwitchGuard guard;

        clusterName = Client_->GetClusterName();
        ASSERT_TRUE(clusterName);
        ASSERT_EQ(*clusterName, ClusterName_);
    }

    for (const auto& transactionType : TEnumTraits<ETransactionType>::GetDomainValues()) {
        auto transaction = WaitFor(Client_->StartTransaction(transactionType))
            .ValueOrThrow();

        TForbidContextSwitchGuard guard;

        clusterName = transaction->GetClient()->GetClusterName();
        ASSERT_TRUE(clusterName);
        ASSERT_EQ(*clusterName, ClusterName_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
