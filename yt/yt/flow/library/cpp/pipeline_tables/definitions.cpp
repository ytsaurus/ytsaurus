#include "public.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>

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
        auto descriptor = descriptorNode->AsMap();
        auto schemaNode = descriptor->GetChildOrThrow("schema");
        auto schemaYson = NYson::ConvertToYsonString(schemaNode);
        TPipelineTableDefinition definition{
            .Schema = ConvertTo<TTableSchemaPtr>(schemaYson),
            .SchemaYson = std::move(schemaYson),
            .Attributes = IAttributeDictionary::FromMap(
                descriptor->GetChildOrThrow("attributes")->AsMap()),
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
        std::map<std::string, TPipelineNodeDefinition> nodes;
        for (const auto& [name, descriptorNode] : root->GetChildOrThrow("nodes")->AsMap()->GetChildren()) {
            auto descriptor = descriptorNode->AsMap();
            TPipelineNodeDefinition definition{
                .Type = ConvertTo<NObjectClient::EObjectType>(descriptor->GetChildOrThrow("type")),
                .Attributes = IAttributeDictionary::FromMap(descriptor->GetChildOrThrow("attributes")->AsMap()),
            };
            nodes.emplace(name, std::move(definition));
        }
        return TPipelineTableDefinitions{
            .Nodes = std::move(nodes),
            .Tables = ParseSection(root, "tables"),
            .Queues = ParseSection(root, "queues"),
        };
    }();
    return Definitions;
}

std::map<std::string, TPipelineNodeDefinition> BuildPipelineNodeDefinitions(
    const TPipelineTableDefinitions& definitions)
{
    auto nodes = definitions.Nodes;
    auto addParentDirectories = [&] (const auto& section) {
        for (const auto& [name, _] : section) {
            for (auto slash = name.find('/'); slash != std::string::npos; slash = name.find('/', slash + 1)) {
                TPipelineNodeDefinition parentDefinition{
                    .Type = NObjectClient::EObjectType::MapNode,
                    .Attributes = CreateEphemeralAttributes(),
                };
                nodes.try_emplace(name.substr(0, slash), std::move(parentDefinition));
            }
        }
    };
    addParentDirectories(definitions.Nodes);
    addParentDirectories(definitions.Tables);
    addParentDirectories(definitions.Queues);
    return nodes;
}

NYPath::TYPath BuildPipelineTablePath(
    NYPath::TYPathBuf pipelinePath,
    TStringBuf relativePath)
{
    NYPath::TYPath path(pipelinePath);
    for (auto segment : StringSplitter(relativePath).Split('/')) {
        path = NYPath::YPathJoin(path, segment.Token());
    }
    return path;
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
