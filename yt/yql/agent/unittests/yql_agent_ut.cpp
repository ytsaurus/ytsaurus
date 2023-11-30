#include <yt/yql/agent/interop.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/formats/skiff_writer.h>
#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NYqlAgent {

using namespace NFormats;
using namespace NHiveClient;
using namespace NNamedValue;
using namespace NSkiff;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TYqlAgentBuildRowsetTest, SkiffFormat)
{
    TClientDirectoryPtr clientDirectory;

    std::vector<TString> columns = {"integer", "string"};
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema(columns[0], EValueType::Int64),
        TColumnSchema(columns[1], EValueType::String)});

    auto nameTable = TNameTable::FromSchema(*tableSchema);

    auto skiffType = BuildYsonStringFluently()
        .BeginAttributes()
            .Item("table_skiff_schemas")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("children")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name")
                            .Value(columns[0])
                            .Item("wire_type")
                            .Value("int64")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name")
                            .Value(columns[1])
                            .Item("wire_type")
                            .Value("string32")
                        .EndMap()
                    .EndList()
                    .Item("wire_type")
                    .Value("tuple")
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("skiff");

    const auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName(columns[0]),
        CreateSimpleTypeSchema(EWireType::String32)->SetName(columns[1]),
    });

    TStringStream data;
    IOutputStream* stream = &data;

    auto skiffWriter = CreateWriterForSkiff(
        {skiffSchema},
        nameTable,
        {tableSchema},
        NConcurrency::CreateAsyncAdapter(stream),
        false,
        New<TControlAttributesConfig>(),
        0);

    auto row1 = MakeRow(nameTable, {{columns[0], 42}, {columns[1], "test1"}});
    auto row2 = MakeRow(nameTable, {{columns[0], 43}, {columns[1], "test2"}});
    std::vector<TUnversionedRow> rows = {row1, row2};

    skiffWriter->Write(rows);
    skiffWriter->Close().Get().ThrowOnError();

    INodePtr resultNode = ConvertTo<INodePtr>(BuildYsonStringFluently()
        .BeginMap()
            .Item("Write")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("SkiffType")
                    .Value(skiffType)
                    .Item("Type")
                    .Value(ConvertToYsonString(tableSchema))
                    .Item("Columns")
                    .Value(ConvertToYsonString(columns))
                    .Item("Data")
                    .Value(data.Str())
                    .Item("Incomplete")
                    .Value(true)
                .EndMap()
            .EndList()
        .EndMap());

    auto yqlRowset = BuildRowset(
        clientDirectory,
        resultNode,
        /*resultIndex*/ 0,
        /*rowCountLimit*/ 10);

    EXPECT_EQ(yqlRowset.ResultRows, rows);
    EXPECT_EQ(*yqlRowset.TargetSchema, *tableSchema);
    EXPECT_EQ(yqlRowset.Incomplete, true);
}


TEST(TYqlAgentBuildRowsetTest, ReorderAndSaveRows)
{
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TString> columns = {"integer", "string"};
    auto sourceTableSchema = New<TTableSchema>(std::vector{
        TColumnSchema(columns[0], EValueType::Int64),
        TColumnSchema(columns[1], EValueType::String)});
    auto sourceNameTable = TNameTable::FromSchema(*sourceTableSchema);

    auto targetTableSchema = New<TTableSchema>(std::vector{
        TColumnSchema(columns[1], EValueType::String),
        TColumnSchema(columns[0], EValueType::Int64)});
    auto targetNameTable = TNameTable::FromSchema(*targetTableSchema);

    auto row = MakeRow(sourceNameTable, {{columns[0], 42}, {columns[1], "test1"}});
    auto expectedRow = MakeRow(targetNameTable, {{columns[1], "test1"}, {columns[0], 42}});

    std::vector<TUnversionedRow> resultRows;
    ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, {row}, resultRows);

    EXPECT_EQ(resultRows, std::vector<TUnversionedRow>{expectedRow});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
