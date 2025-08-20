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

std::vector<const TAccessControlDescriptor*> TAcdFetcher::Fetch(
    TRange<TRange<TCypressNodeDescriptor>> joinedDescriptors)
{
    auto nodeIds = joinedDescriptors
        | std::views::join
        | std::views::transform(
            [] (const TCypressNodeDescriptor& descriptor) -> TNodeId {
            return descriptor.Id;
        });

    std::vector<TNodeId> missingAcds;
    int nodeCount = 0;
    for (auto nodeId : nodeIds) {
        if (!NodeIdToAcd_.contains(nodeId)) {
            missingAcds.push_back(nodeId);
        }
        ++nodeCount;
    }

    if (!missingAcds.empty()) {
        auto fetchedAcds = FetchAcds(missingAcds, SequoiaTransaction_);
        for (auto&& [nodeId, acd] : Zip(missingAcds, fetchedAcds)) {
            if (!acd.has_value()) {
                YT_LOG_DEBUG(
                    "Failed to fetch ACD entry from Sequoia tables for node: entry not found (NodeId: %v)",
                    nodeId);
                // NB: Stale read of an object ACD is fine. Default descriptor
                // acts as no permission rights were configured.
                acd = MakeDefaultDescriptor(nodeId);
            }
            NodeIdToAcd_.emplace(nodeId, std::move(*acd));
        }
    }

    std::vector<const TAccessControlDescriptor*> result(nodeCount);
    std::transform(
        nodeIds.begin(),
        nodeIds.end(),
        result.begin(),
        [&] (auto nodeId) -> const TAccessControlDescriptor* {
            const auto& acd = GetOrCrash(NodeIdToAcd_, nodeId);
            return &acd;
        });

    return result;
}

TAccessControlDescriptor TAcdFetcher::MakeDefaultDescriptor(TNodeId nodeId)
{
    return {.NodeId = nodeId, .Inherit = true};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
