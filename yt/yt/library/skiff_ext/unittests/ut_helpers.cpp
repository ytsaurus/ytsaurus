#include "ut_helpers.h"

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/ytree/node.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <library/cpp/yt/string/stream.h>

namespace NYT::NSkiffExt {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string ConvertToSkiffSchemaShortDebugString(const INodePtr& node)
{
    auto mapNode = node->AsMap();
    auto registryNode = mapNode->FindChild("skiff_schema_registry");
    auto skiffSchemas = ParseSkiffSchemas(
        registryNode ? registryNode->AsMap() : nullptr,
        mapNode->GetChildOrThrow("table_skiff_schemas")->AsList());

    TStdStringStream result;
    result << '{';
    for (const auto& schema : skiffSchemas) {
        result << NSkiff::GetShortDebugString(schema);
        result << ',';
    }
    result << '}';
    return result.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiffExt
