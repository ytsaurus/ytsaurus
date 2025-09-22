#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NCypressProxy {

class ISequoiaAttributeFetcher
    : public TRefCounted
{
public:
    virtual ~ISequoiaAttributeFetcher() = default;

    virtual TFuture<THashMap<NCypressClient::TNodeId, NYTree::INodePtr>> FetchNodesWithAttributes() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaAttributeFetcher);

////////////////////////////////////////////////////////////////////////////////

ISequoiaAttributeFetcherPtr CreateAttributeFetcherForGetRequest(
    const NApi::NNative::IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    const NCypressClient::TNodeId rootId,
    const std::vector<NCypressClient::TNodeId>& nodesToFetchFromMaster);

ISequoiaAttributeFetcherPtr CreateAttributeFetcherForListRequest(
    const NApi::NNative::IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    const NCypressClient::TNodeId rootId,
    const std::vector<TCypressChildDescriptor>& children);

std::tuple<ISequoiaAttributeFetcherPtr, NYTree::TAttributeFilter> CreateSpecialAttributeFetcherAndLeftAttributesForNode(
    const NApi::NNative::IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const NYTree::TAttributeFilter& attributeFilter,
    const NCypressClient::TNodeId rootId);

} // namespace NYT::NCypressServer
