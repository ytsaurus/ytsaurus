#include "acd_fetcher.h"

#include "action_helpers.h"
#include "private.h"

#include <library/cpp/iterator/zip.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TAcdFetcher::TAcdFetcher(ISequoiaTransactionPtr sequoiaTransaction)
    : SequoiaTransaction_(std::move(sequoiaTransaction))
{ }

std::vector<const TAccessControlDescriptor*> DoFetch(
    ISequoiaTransactionPtr sequoiaTransaction,
    auto nodeIds,
    THashMap<TNodeId, TAccessControlDescriptor>& nodeIdToAcd)
{
    std::vector<TNodeId> missingAcds;
    int nodeCount = 0;
    for (auto nodeId : nodeIds) {
        if (!nodeIdToAcd.contains(nodeId)) {
            missingAcds.push_back(nodeId);
        }
        ++nodeCount;
    }

    if (!missingAcds.empty()) {
        auto fetchedAcds = FetchAcds(missingAcds, sequoiaTransaction);
        for (auto&& [nodeId, acd] : Zip(missingAcds, fetchedAcds)) {
            if (!acd.has_value()) {
                YT_LOG_DEBUG(
                    "Unable to fetch ACD entry from Sequoia tables for node: entry not found (NodeId: %v)",
                    nodeId);
                // NB: Stale read of an object ACD is fine. Default descriptor
                // acts as no permission rights were configured.
                acd = TAccessControlDescriptor{.NodeId = nodeId};
            }
            nodeIdToAcd.emplace(nodeId, std::move(*acd));
        }
    }

    std::vector<const TAccessControlDescriptor*> result(nodeCount);
    std::transform(
        nodeIds.begin(),
        nodeIds.end(),
        result.begin(),
        [&] (auto nodeId) -> const TAccessControlDescriptor* {
            const auto& acd = GetOrCrash(nodeIdToAcd, nodeId);
            return &acd;
        });

    return result;
}

std::vector<const TAccessControlDescriptor*> TAcdFetcher::Fetch(
    TRange<TRange<TCypressNodeDescriptor>> joinedDescriptors)
{
    auto nodeIds = joinedDescriptors
        | std::views::join
        | std::views::transform(
            [] (const TCypressNodeDescriptor& descriptor) -> TNodeId {
            return descriptor.Id;
        });
    return DoFetch(SequoiaTransaction_, nodeIds, NodeIdToAcd_);
}

std::vector<const TAccessControlDescriptor*> TAcdFetcher::Fetch(
    TRange<std::vector<TCypressChildDescriptor>> joinedDescriptors)
{
    auto nodeIds = joinedDescriptors
        | std::views::join
        | std::views::transform(
            [] (const TCypressChildDescriptor& descriptor) -> TNodeId {
            return descriptor.ChildId;
        });
    return DoFetch(SequoiaTransaction_, nodeIds, NodeIdToAcd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
