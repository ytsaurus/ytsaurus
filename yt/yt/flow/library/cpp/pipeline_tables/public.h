#pragma once

#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/public.h>

#include <map>
#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TPipelineNodeDefinition
{
    NObjectClient::EObjectType Type = NObjectClient::EObjectType::Null;
    NYTree::IAttributeDictionaryPtr Attributes;
};

struct TPipelineTableDefinition
{
    NTableClient::TTableSchemaPtr Schema;
    NYson::TYsonString SchemaYson;
    NYTree::IAttributeDictionaryPtr Attributes;
};

struct TPipelineTableDefinitions
{
    std::map<std::string, TPipelineNodeDefinition> Nodes;
    std::map<std::string, TPipelineTableDefinition> Tables;
    std::map<std::string, TPipelineTableDefinition> Queues;
};

const TPipelineTableDefinitions& GetPipelineTableDefinitions();

std::map<std::string, TPipelineNodeDefinition> BuildPipelineNodeDefinitions(
    const TPipelineTableDefinitions& definitions);

NYPath::TYPath BuildPipelineTablePath(
    NYPath::TYPathBuf pipelinePath,
    TStringBuf relativePath);

NYTree::IAttributeDictionaryPtr BuildPipelineTableAttributes(
    const TPipelineTableDefinition& definition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
