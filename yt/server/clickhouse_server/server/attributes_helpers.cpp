#include "attributes_helpers.h"

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

NInterop::EObjectType ToObjectType(const TString& typeName)
{
    if (typeName == "table") {
        return NInterop::EObjectType::Table;
    } else if (typeName == "file") {
        return NInterop::EObjectType::File;
    } else if (typeName == "document") {
        return NInterop::EObjectType::Document;
    } else {
        return NInterop::EObjectType::Other;
    }
}

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

NInterop::TObjectAttributes CreateBasicAttributes(const IMapNode& attributeMap)
{
    NInterop::TObjectAttributes attributes;

    attributes.Type = ToObjectType(
        attributeMap.GetChild("type")->AsString()->GetValue());

    attributes.Revision = attributeMap.GetChild("revision")->AsInt64()->GetValue();

    attributes.LastModificationTime = TInstant::ParseIso8601Deprecated(
        attributeMap.GetChild("modification_time")->AsString()->GetValue());

    return attributes;
}

NInterop::TObjectAttributes CreateBasicAttributes(
    const IAttributeDictionary& attributeDictionary)
{
    const auto attributeMap = attributeDictionary.ToMap();
    return CreateBasicAttributes(*attributeMap);
}

} // namespace NClickHouse
} // namespace NYT
