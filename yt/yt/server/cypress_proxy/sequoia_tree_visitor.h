#pragma once

#include <yt/ytlib/sequoia_client/records/child_node.record.h>

#include<yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    NCypressClient::TNodeId rootId,
    int maxDepth,
    NYson::IYsonConsumer* consumer,
    const NYTree::TAttributeFilter& attributeFilter,
    const THashMap<NCypressClient::TNodeId, std::vector<NSequoiaClient::NRecords::TChildNode>>& nodeIdToChildren,
    const THashMap<NCypressClient::TNodeId, NYTree::TYPathProxy::TRspGetPtr>& nodeIdToMasterResponse);

// NB: Same as the above, but using async consumer instead of a sync one.
void VisitSequoiaTree(
    NCypressClient::TNodeId rootId,
    int maxDepth,
    NYson::IAsyncYsonConsumer* consumer,
    const NYTree::TAttributeFilter& attributeFilter,
    const THashMap<NCypressClient::TNodeId, std::vector<NSequoiaClient::NRecords::TChildNode>>& nodeIdToChildren,
    const THashMap<NCypressClient::TNodeId, NYTree::TYPathProxy::TRspGetPtr>& nodeIdToMasterResponse);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
