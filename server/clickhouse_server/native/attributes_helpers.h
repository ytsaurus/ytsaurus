#pragma once

#include "objects.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/node.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

TString GetAttributesRootPath(const TString& path);
TString GetAttributePath(const TString& path, const TString& attributeName);

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetBasicAttributesKeys();

TObjectAttributes CreateBasicAttributes(
    const NYTree::IMapNode& attributeMap);

TObjectAttributes CreateBasicAttributes(
    const NYTree::IAttributeDictionary& attributeDictionary);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T GetAttribute(NYTree::INodePtr node, const TString& name)
{
    const auto& attributes = node->Attributes();
    return attributes.Get<T>(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
