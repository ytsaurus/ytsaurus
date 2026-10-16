#include "describe_traits.h"

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<std::string> FindString(const IMapNodePtr& parameters, const std::string& key)
{
    auto node = parameters->FindChild(key);
    if (!node || node->GetType() != ENodeType::String) {
        return std::nullopt;
    }
    return node->AsString()->GetValue();
}

std::optional<std::string> FindFirstListString(const IMapNodePtr& parameters, const std::string& key)
{
    auto node = parameters->FindChild(key);
    if (!node || node->GetType() != ENodeType::List) {
        return std::nullopt;
    }
    auto first = node->AsList()->FindChild(0);
    if (!first || first->GetType() != ENodeType::String) {
        return std::nullopt;
    }
    return first->AsString()->GetValue();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TClickHouseDescribeTraits::MakeLinks(const IMapNodePtr& parameters) const
{
    TDescribeTraitsBase::MakeLinks(parameters);

    auto host = FindString(parameters, "host");
    if (!host) {
        host = FindFirstListString(parameters, "hosts");
    }
    auto database = FindString(parameters, "database");
    auto table = FindString(parameters, "table");
    if (!host || !table) {
        return;
    }

    auto port = parameters->FindChild("port");
    std::string portText = ":9000";
    if (port && port->GetType() == ENodeType::Int64) {
        portText = Format(":%v", port->AsInt64()->GetValue());
    }
    auto databaseText = database.value_or("default");

    parameters->AddChild(
        "target",
        BuildYsonNodeFluently()
            .Value(Format("%v%v/%v.%v", *host, portText, databaseText, *table)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
