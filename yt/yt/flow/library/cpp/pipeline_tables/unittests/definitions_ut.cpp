#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
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
    checkDefinitions("tables", definitions.Tables);
    checkDefinitions("queues", definitions.Queues);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
