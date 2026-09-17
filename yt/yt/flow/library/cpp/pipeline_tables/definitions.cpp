#include "public.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/resource/resource.h>

#include <utility>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TStringBuf DefinitionsResourceKey =
    "resfs/file/yt/yt/flow/library/pipeline_tables/definitions.yson";

std::map<std::string, TPipelineTableDefinition> ParseSection(
    const IMapNodePtr& root,
    TStringBuf section)
{
    std::map<std::string, TPipelineTableDefinition> result;
    for (const auto& [name, descriptorNode] : root->GetChildOrThrow(section)->AsMap()->GetChildren()) {
        auto schemaNode = descriptorNode->AsMap()->GetChildOrThrow("schema");
        auto schemaYson = NYson::ConvertToYsonString(schemaNode);
        TPipelineTableDefinition definition{
            .Schema = ConvertTo<TTableSchemaPtr>(schemaYson),
            .SchemaYson = std::move(schemaYson),
            .Attributes = IAttributeDictionary::FromMap(
                descriptorNode->AsMap()->GetChildOrThrow("attributes")->AsMap()),
        };
        result.emplace(name, std::move(definition));
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TPipelineTableDefinitions& GetPipelineTableDefinitions()
{
    static const auto Definitions = [] {
        auto root = ConvertTo<INodePtr>(NYson::TYsonString(NResource::Find(DefinitionsResourceKey)))->AsMap();
        return TPipelineTableDefinitions{
            .Tables = ParseSection(root, "tables"),
            .Queues = ParseSection(root, "queues"),
        };
    }();
    return Definitions;
}

IAttributeDictionaryPtr BuildPipelineTableAttributes(
    const TPipelineTableDefinition& definition)
{
    auto attributes = definition.Attributes->Clone();
    attributes->SetYson("schema", definition.SchemaYson);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
