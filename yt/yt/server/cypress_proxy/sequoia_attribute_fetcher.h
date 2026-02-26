#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

bool HasSpecialAttributes(const NYTree::TAttributeFilter& attributeFilter);

////////////////////////////////////////////////////////////////////////////////

TFuture<TNodeIdToAttributes> FetchAttributesForGetRequest(
    const TSequoiaSessionPtr& sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    NCypressClient::TNodeId rootId,
    const TNodeIdToChildDescriptors* nodeIdToChildren,
    TNodeAncestry rootAncestry,
    const std::vector<NCypressClient::TNodeId>* scalarNodeIds);

TFuture<TNodeIdToAttributes> FetchAttributesForListRequest(
    const TSequoiaSessionPtr& sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    NCypressClient::TNodeId rootId,
    const std::vector<TCypressChildDescriptor>* children,
    TNodeAncestry rootAncestry);

TFuture<NYTree::IAttributeDictionaryPtr> FetchAttributesForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    NCypressClient::TNodeId rootId,
    TNodeAncestry rootAncestry);

} // namespace NYT::NCypressServer
