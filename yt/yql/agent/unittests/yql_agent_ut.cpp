#include <yt/yql/agent/interop.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NYqlAgent {

using namespace NFormats;
using namespace NHiveClient;
using namespace NNamedValue;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

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
