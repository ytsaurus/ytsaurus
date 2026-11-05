#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/resource/resource.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TPipelineTableDefinitionsTest, LoadsSchemasAndAttributesFromResource)
{
    const auto& definitions = GetPipelineTableDefinitions();
    auto resource = ConvertTo<INodePtr>(NYson::TYsonString(NResource::Find(
        "resfs/file/yt/yt/flow/library/pipeline_tables/definitions.yson")))
        ->AsMap();

    auto checkDefinitions = [&] (TStringBuf sectionName, const auto& section) {
        auto resourceSection = resource->GetChildOrThrow(sectionName)->AsMap();
        ASSERT_EQ(section.size(), resourceSection->GetChildCount());
        for (const auto& [name, definition] : section) {
            auto descriptor = resourceSection->GetChildOrThrow(name)->AsMap();
            EXPECT_EQ(
                *ConvertTo<TTableSchemaPtr>(definition.SchemaYson),
                *definition.Schema);
            EXPECT_TRUE(AreNodesEqual(
                ConvertTo<INodePtr>(definition.SchemaYson),
                descriptor->GetChildOrThrow("schema")));
            EXPECT_TRUE(AreNodesEqual(
                definition.Attributes->ToMap(),
                descriptor->GetChildOrThrow("attributes")));
        }
    };
    ASSERT_EQ(definitions.Nodes.size(), resource->GetChildOrThrow("nodes")->AsMap()->GetChildCount());
    for (const auto& [name, definition] : definitions.Nodes) {
        auto descriptor = resource->GetChildOrThrow("nodes")->AsMap()->GetChildOrThrow(name)->AsMap();
        EXPECT_EQ(definition.Type, ConvertTo<NObjectClient::EObjectType>(descriptor->GetChildOrThrow("type")));
        EXPECT_TRUE(AreNodesEqual(
            definition.Attributes->ToMap(),
            descriptor->GetChildOrThrow("attributes")));
    }
    checkDefinitions("tables", definitions.Tables);
    checkDefinitions("queues", definitions.Queues);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPipelineTableDefinitionsTest, PreservesExplicitNodesAndAddsImplicitMapParents)
{
    TPipelineTableDefinitions definitions;
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("custom_attribute", "value");
    definitions.Nodes["nodes/deep"] = {NObjectClient::EObjectType::Document, attributes};
    definitions.Nodes["shared"] = {NObjectClient::EObjectType::MapNode, attributes};
    definitions.Tables["shared/deep/table"] = {};
    definitions.Tables["shared/other"] = {};
    definitions.Queues["queues/deep/queue"] = {};
    definitions.Queues["shared/deep/queue"] = {};
    definitions.Queues["flat_queue"] = {};

    auto nodes = BuildPipelineNodeDefinitions(definitions);
    EXPECT_THAT(
        nodes,
        ::testing::ElementsAre(
            ::testing::Key("nodes"),
            ::testing::Key("nodes/deep"),
            ::testing::Key("queues"),
            ::testing::Key("queues/deep"),
            ::testing::Key("shared"),
            ::testing::Key("shared/deep")));
    for (const auto& [name, definition] : nodes) {
        if (auto it = definitions.Nodes.find(name); it != definitions.Nodes.end()) {
            EXPECT_EQ(definition.Type, it->second.Type);
            EXPECT_TRUE(AreNodesEqual(definition.Attributes->ToMap(), it->second.Attributes->ToMap()));
        } else {
            EXPECT_EQ(definition.Type, NObjectClient::EObjectType::MapNode);
            EXPECT_TRUE(definition.Attributes->ListKeys().empty());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
