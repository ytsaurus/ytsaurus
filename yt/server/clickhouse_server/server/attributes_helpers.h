#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/node.h>

#include <util/generic/string.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TString GetAttributesRootPath(const TString& path);
TString GetAttributePath(const TString& path, const TString& attributeName);

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetBasicAttributesKeys();

NInterop::TObjectAttributes CreateBasicAttributes(
    const NYT::NYTree::IMapNode& attributeMap);

NInterop::TObjectAttributes CreateBasicAttributes(
    const NYT::NYTree::IAttributeDictionary& attributeDictionary);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T GetAttribute(NYTree::INodePtr node, const TString& name)
{
    const auto& attributes = node->Attributes();
    return attributes.Get<T>(name);
}

} // namespace NClickHouse
} // namespace NYT
