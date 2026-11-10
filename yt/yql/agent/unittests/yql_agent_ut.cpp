#include <yt/yql/agent/interop.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

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

TEST(TQuerySettingsLoggingTest, RemovesTokensAndPreservesSettings)
{
    auto settings = BuildYsonStringFluently()
        .BeginMap()
            .Item("cluster").Value("hahn")
            .Item("tokens").BeginMap()
                .Item("yql_service_token").Value("service-secret")
            .EndMap()
            .Item("declared_parameters").Value("parameters")
        .EndMap();

    auto sanitized = SanitizeQuerySettingsForLogging(settings);
    ASSERT_TRUE(sanitized);

    auto node = ConvertTo<IMapNodePtr>(*sanitized);
    EXPECT_FALSE(node->FindChild("tokens"));
    EXPECT_EQ(node->GetChildValueOrThrow<std::string>("cluster"), "hahn");
    EXPECT_EQ(node->GetChildValueOrThrow<std::string>("declared_parameters"), "parameters");

    EXPECT_TRUE(ConvertTo<IMapNodePtr>(settings)->FindChild("tokens"));
}

TEST(TQuerySettingsLoggingTest, SanitizesEmbeddedTaskData)
{
    NYql::NProto::TTaskData taskData;
    taskData.SetId("test-query-id");
    taskData.SetAuthData("auth-secret");
    taskData.MutableUserAuthData()->SetOauthToken("oauth-secret");
    taskData.MutableUserAuthData()->SetBlackboxSessionIdCookie("cookie-secret");
    taskData.SetGatewaysConfig("gateway-secret");
    taskData.SetGatewaysConfigPatch("patch-secret");
    taskData.SetParameters("parameters");
    taskData.SetAttributes("attributes");

    auto settings = BuildYsonStringFluently()
        .BeginMap()
            .Item("tokens").BeginMap()
                .Item("yql_service_token").Value("service-secret")
            .EndMap()
            .Item("yql_task_data").Value(taskData.SerializeAsString())
        .EndMap();

    auto sanitized = SanitizeQuerySettingsForLogging(settings);
    ASSERT_TRUE(sanitized);

    auto node = ConvertTo<IMapNodePtr>(*sanitized);
    EXPECT_FALSE(node->FindChild("tokens"));

    auto data = node->GetChildOrThrow("yql_task_data")->AsMap();
    EXPECT_EQ(data->GetChildValueOrThrow<std::string>("Id"), "test-query-id");
    EXPECT_EQ(data->GetChildValueOrThrow<std::string>("Parameters"), "parameters");
    EXPECT_EQ(data->GetChildValueOrThrow<std::string>("Attributes"), "attributes");
    EXPECT_FALSE(data->FindChild("AuthData"));
    EXPECT_FALSE(data->FindChild("UserAuthData"));
    EXPECT_FALSE(data->FindChild("GatewaysConfig"));
    EXPECT_FALSE(data->FindChild("GatewaysConfigPatch"));
    EXPECT_EQ(ConvertToYsonString(node, EYsonFormat::Text).ToString().find("secret"), std::string::npos);

    auto original = ConvertTo<IMapNodePtr>(settings);
    EXPECT_TRUE(original->FindChild("tokens"));
    EXPECT_EQ(original->GetChildValueOrThrow<TString>("yql_task_data"), taskData.SerializeAsString());
}

TEST(TQuerySettingsLoggingTest, RejectsMalformedInputWithoutExposingIt)
{
    for (auto input : {
        "{invalid-secret",
        "[]",
        "{yql_task_data=123;}",
        "{yql_task_data=invalid-secret;}",
    }) {
        EXPECT_FALSE(SanitizeQuerySettingsForLogging(TYsonString(TStringBuf(input))));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
