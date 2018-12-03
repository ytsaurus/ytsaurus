#include "attributes_helpers.h"

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

EObjectType ToObjectType(const TString& typeName)
{
    if (typeName == "table") {
        return EObjectType::Table;
    } else if (typeName == "file") {
        return EObjectType::File;
    } else if (typeName == "document") {
        return EObjectType::Document;
    } else {
        return EObjectType::Other;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TString GetAttributesRootPath(const TString& path)
{
    return path + "/@";
}

TString GetAttributePath(const TString& path, const TString& attributeName)
{
    return path + "/@" + attributeName;
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetBasicAttributesKeys()
{
    static std::vector<TString> KEYS = {
        "type",
        "revision",
        "modification_time"
    };
    return KEYS;
}

TObjectAttributes CreateBasicAttributes(const IMapNode& attributeMap)
{
    TObjectAttributes attributes;

    attributes.Type = ToObjectType(
        attributeMap.GetChild("type")->AsString()->GetValue());

    auto child = attributeMap.GetChild("revision");
    if (child->GetType() == ENodeType::Int64) {
        attributes.Revision = attributeMap.GetChild("revision")->AsInt64()->GetValue();
    } else {
        attributes.Revision = attributeMap.GetChild("revision")->AsUint64()->GetValue();
    }

    attributes.LastModificationTime = TInstant::ParseIso8601Deprecated(
        attributeMap.GetChild("modification_time")->AsString()->GetValue());

    return attributes;
}

TObjectAttributes CreateBasicAttributes(
    const IAttributeDictionary& attributeDictionary)
{
    const auto attributeMap = attributeDictionary.ToMap();
    return CreateBasicAttributes(*attributeMap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
