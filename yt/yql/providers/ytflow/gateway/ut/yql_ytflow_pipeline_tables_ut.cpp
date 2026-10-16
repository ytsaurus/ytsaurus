#include <yt/yql/providers/ytflow/gateway/yql_ytflow_prepare_yt.h>

#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYql::NYtflow::NPrepare::NPrivate {
namespace {

using namespace NYT::NTableClient;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TYqlPipelineTablesTest, UsesCanonicalSchemas)
{
    auto tables = BuildYqlPipelineTableAttributes(/*createWorkerLogsTable*/ false);
    const auto& definitions = NYT::NFlow::GetPipelineTableDefinitions();
    ASSERT_EQ(tables.size(), definitions.Tables.size() + definitions.Queues.size());

    THashMap<TString, IAttributeDictionaryPtr> attributesByName;
    for (auto& [name, attributes] : tables) {
        attributesByName.emplace(name, std::move(attributes));
    }

    auto checkDefinitions = [&] (const auto& section) {
        for (const auto& [name, definition] : section) {
            auto attributes = attributesByName.at(name);
            auto commonAttributes = attributes->Clone();
            auto schemaYson = commonAttributes->GetYsonAndRemove("schema");
            EXPECT_EQ(*ConvertTo<TTableSchemaPtr>(schemaYson), *definition.Schema);
            EXPECT_TRUE(AreNodesEqual(
                ConvertTo<INodePtr>(schemaYson),
                ConvertTo<INodePtr>(definition.SchemaYson)));
            EXPECT_TRUE(AreNodesEqual(
                commonAttributes->ToMap(),
                definition.Attributes->ToMap()));
        }
    };
    checkDefinitions(definitions.Tables);
    checkDefinitions(definitions.Queues);
}

TEST(TYqlPipelineTablesTest, WorkerLogsReuseControllerLogsSchema)
{
    auto tables = BuildYqlPipelineTableAttributes(/*createWorkerLogsTable*/ true);
    const auto& definitions = NYT::NFlow::GetPipelineTableDefinitions();
    ASSERT_EQ(tables.size(), definitions.Tables.size() + definitions.Queues.size() + 1);

    THashMap<TString, IAttributeDictionaryPtr> attributesByName;
    for (auto& [name, attributes] : tables) {
        attributesByName.emplace(name, std::move(attributes));
    }

    const auto& controllerLogs = attributesByName.at("controller_logs");
    const auto& workerLogs = attributesByName.at("worker_logs");
    EXPECT_TRUE(AreNodesEqual(controllerLogs->ToMap(), workerLogs->ToMap()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYql::NYtflow::NPrepare::NPrivate
