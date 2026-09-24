#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/public.h>

#include <map>
#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TPipelineTableDefinition
{
    NTableClient::TTableSchemaPtr Schema;
    NYson::TYsonString SchemaYson;
    NYTree::IAttributeDictionaryPtr Attributes;
};

struct TPipelineTableDefinitions
{
    std::map<std::string, TPipelineTableDefinition> Tables;
    std::map<std::string, TPipelineTableDefinition> Queues;
};

const TPipelineTableDefinitions& GetPipelineTableDefinitions();

NYTree::IAttributeDictionaryPtr BuildPipelineTableAttributes(
    const TPipelineTableDefinition& definition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
