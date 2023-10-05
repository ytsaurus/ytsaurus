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

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <tuple>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NYTree;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TTypeV3Test : public TDynamicTablesTestBase
{
public:
    static void SetTablePath(const TYPath& path)
    {
        Table_ = path;
    }
};

TEST_F(TTypeV3Test, TestCreateStaticTable)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("key", SimpleLogicalType(ESimpleLogicalValueType::String)),
        TColumnSchema("value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });
    TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("schema", schema);
    WaitFor(Client_->CreateNode("//tmp/f", NObjectClient::EObjectType::Table, options))
        .ThrowOnError();

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> writtenData;

    {
        auto tableWriter = WaitFor(Client_->CreateTableWriter(NYPath::TRichYPath("//tmp/f")))
            .ValueOrThrow();

        auto nameTable = tableWriter->GetNameTable();
        auto writerSchema = tableWriter->GetSchema();
        EXPECT_EQ(*writerSchema, *schema);
        auto createRow = [&] (TStringBuf key, TStringBuf value) {
            TUnversionedOwningRowBuilder builder;

            builder.AddValue(MakeUnversionedStringValue(key, nameTable->GetIdOrRegisterName("key")));
            builder.AddValue(MakeUnversionedCompositeValue(value, nameTable->GetIdOrRegisterName("value")));

            return rowBuffer->CaptureRow(builder.FinishRow().Get());
        };

        writtenData = {
            createRow("foo", "[3; 4; 5]"),
            createRow("bar", "[6; 7]"),
        };
        auto written = tableWriter->Write(MakeRange<TUnversionedRow>(writtenData));
        EXPECT_EQ(written, true);

        WaitFor(tableWriter->Close())
            .ThrowOnError();
    }

    {
        auto tableReader = WaitFor(Client_->CreateTableReader(NYPath::TRichYPath("//tmp/f")))
            .ValueOrThrow();

        auto readerSchema = tableReader->GetTableSchema();
        EXPECT_EQ(*readerSchema, *schema);

        std::vector<TUnversionedRow> tableRows;
        while (auto batch = tableReader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
            }

            for (const auto row : batch->MaterializeRows()) {
                tableRows.emplace_back(rowBuffer->CaptureRow(row));
            }
        }

        auto toStringVector = [] (const std::vector<TUnversionedRow>& rows) {
            std::vector<TString> result;
            for (const auto& r : rows) {
                result.push_back(ToString(r));
            }
            return result;
        };

        EXPECT_EQ(toStringVector(tableRows), toStringVector(writtenData));
    }
}

void CreateDynamicTable(
    const NApi::IClientPtr& client,
    const TString& path,
    const TTableSchema& schema,
    const std::vector<std::pair<TString, TString>>& attributes = {})
{
    TTypeV3Test::SetTablePath(path);

    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("schema", schema);
    options.Attributes->Set("dynamic", true);
    for (const auto& [key, value] : attributes) {
        options.Attributes->Set(key, value);
    }
    options.Force = true;
    WaitFor(client->CreateNode(path, EObjectType::Table, options))
        .ThrowOnError();
}

TEST_F(TTypeV3Test, TestCreateDynamicTable)
{
    static const TYPath path = "//tmp/dynamic-table";
    auto createKvDynamicTable = [] (const TLogicalTypePtr& keyType, const TLogicalTypePtr& valueType) {
        CreateDynamicTable(
            Client_,
            path,
            *TTableSchema(std::vector<TColumnSchema>({
                {"key", keyType, ESortOrder::Ascending},
                {"value", valueType},
            })).SetUniqueKeys(true)
        );
    };

    EXPECT_NO_THROW(
        createKvDynamicTable(
            SimpleLogicalType(ESimpleLogicalValueType::String),
            ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))
        )
    );

    EXPECT_NO_THROW(
        createKvDynamicTable(
            SimpleLogicalType(ESimpleLogicalValueType::String),
            DecimalLogicalType(3, 2)
        )
    );

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        createKvDynamicTable(
            ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
            SimpleLogicalType(ESimpleLogicalValueType::String)
        ),
        std::exception,
        "Dynamic table cannot have key column of type");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        createKvDynamicTable(
            DecimalLogicalType(3, 2),
            SimpleLogicalType(ESimpleLogicalValueType::String)
        ),
        std::exception,
        "Dynamic table cannot have key column of type");
}

struct TTypeV3TestWithOptimizedFor
    : public TTypeV3Test
    , public testing::WithParamInterface<TString>
{ };

static std::string PrintOptimizedFor(const ::testing::TestParamInfo<TString>& info)
{
    auto data = info.param;
    return {data.Data(), data.Size()};
}

INSTANTIATE_TEST_SUITE_P(
    OptimizeFor,
    TTypeV3TestWithOptimizedFor,
    testing::Values("scan", "lookup"),
    PrintOptimizedFor
);

TEST_P(TTypeV3TestWithOptimizedFor, TestLookup)
{
    const auto optimizeFor = GetParam();

    //
    // Create table
    auto path = "//tmp/dynamic-table";
    CreateDynamicTable(
        Client_,
        path,
        *TTableSchema(std::vector<TColumnSchema>{
            {"key", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
            {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
        }).SetUniqueKeys(true),
        {{"optimize_for", optimizeFor}}
    );

    auto nameTable = New<TNameTable>();

    SyncMountTable(path);

    //
    // Write data
    auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    const TUnversionedOwningRow row = NNamedValue::MakeRow(nameTable, {
        {"key", 1},
        {"value", EValueType::Composite, "[1;2;3]"},
    });

    auto rowRange = MakeSharedRange(std::vector<TUnversionedRow>{row}, nameTable);
    transaction->WriteRows(path, nameTable, rowRange);

    WaitFor(transaction->Commit())
        .ThrowOnError();

    auto checkRows = [&] {
        auto rowset = WaitFor(
            Client_->LookupRows(
                path,
                nameTable,
                MakeSharedRange(
                    std::vector<TLegacyKey>({
                        NNamedValue::MakeRow(nameTable, {{"key", 1}})
                    }),
                    nameTable
                )))
            .ValueOrThrow()
            .Rowset;

        auto actual = rowset->GetRows().ToVector();
        auto expected = std::vector<TUnversionedRow>({row});
        EXPECT_EQ(expected, actual);
    };

    //
    // Read data
    checkRows();

    SyncUnmountTable(path);
    SyncMountTable(path);

    checkRows();

    auto selected = WaitFor(Client_->SelectRows("to_any(value) FROM [//tmp/dynamic-table]"))
        .ValueOrThrow();

    EXPECT_EQ(
        selected.Rowset->GetRows().ToVector(),
        std::vector<TUnversionedRow>({
            NNamedValue::MakeRow(nameTable, {
                {"value", EValueType::Composite, "[1;2;3]"}
            })
        })
    );
}

TEST_P(TTypeV3TestWithOptimizedFor, TestOrdered)
{
    const auto optimizeFor = GetParam();
    //
    // Create table
    auto path = "//tmp/dynamic-table";
    CreateDynamicTable(
        Client_,
        path,
        TTableSchema(std::vector<TColumnSchema>({
            {"key", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })),
        {{"optimize_for", optimizeFor}}
    );

    auto nameTable = New<TNameTable>();

    SyncMountTable(path);

    //
    // Write data
    auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    const TUnversionedOwningRow row = NNamedValue::MakeRow(nameTable, {
        {"key", 1},
        {"value", EValueType::Composite, "[1;2;3]"},
    });

    auto rowRange = MakeSharedRange(std::vector<TUnversionedRow>{row}, nameTable);
    transaction->WriteRows(path, nameTable, rowRange);

    WaitFor(transaction->Commit())
        .ThrowOnError();

    auto checkRows = [&] {
        auto selected = WaitFor(Client_->SelectRows("to_any(value) FROM [//tmp/dynamic-table]"))
            .ValueOrThrow();

        EXPECT_EQ(
            selected.Rowset->GetRows().ToVector(),
            std::vector<TUnversionedRow>({
                NNamedValue::MakeRow(nameTable, {
                    {"value", EValueType::Composite, "[1;2;3]"}
                })
            })
        );
    };

    //
    // Read data
    checkRows();

    SyncUnmountTable(path);
    SyncMountTable(path);

    checkRows();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
