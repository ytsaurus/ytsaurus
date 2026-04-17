#include "node_status_directory.h"

#include <yt/yt/client/chunk_client/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

bool IsSuspiciousNodeError(const TError& error)
{
    return
        error.FindMatching(NRpc::EErrorCode::TransportError) ||
        error.FindMatching(NChunkClient::EErrorCode::MasterNotConnected);
}

////////////////////////////////////////////////////////////////////////////////

class TNodeStatusDirectory
    : public INodeStatusDirectory
{
public:
    explicit TNodeStatusDirectory(NLogging::TLogger logger)
        : Logger(std::move(logger))
    { }

    void UpdateSuspicionMarkTime(
        TNodeId nodeId,
        TStringBuf address,
        bool suspicious,
        std::optional<TInstant> previousMarkTime) override
    {
        auto guard = WriterGuard(SuspiciousNodesSpinLock_);

        auto it = SuspiciousNodesMarkTime_.find(nodeId);
        if (it == SuspiciousNodesMarkTime_.end() && suspicious) {
            YT_LOG_DEBUG("Node is marked as suspicious (NodeId: %v, Address: %v)",
                nodeId,
                address);
            SuspiciousNodesMarkTime_[nodeId] = TInstant::Now();
        }
        if (it != SuspiciousNodesMarkTime_.end() &&
            previousMarkTime == it->second &&
            !suspicious)
        {
            YT_LOG_DEBUG("Node is not suspicious anymore (NodeId: %v, Address: %v)",
                nodeId,
                address);
            SuspiciousNodesMarkTime_.erase(nodeId);
        }
    }

    THashMap<TNodeId, TInstant> RetrieveSuspicionMarkTimes(
        TRange<TNodeId> nodeIds) const override
    {
        if (nodeIds.empty()) {
            return {};
        }

        THashMap<TNodeId, TInstant> nodeIdToSuspicionMarkTime;

        auto guard = ReaderGuard(SuspiciousNodesSpinLock_);

        for (auto nodeId : nodeIds) {
            auto it = SuspiciousNodesMarkTime_.find(nodeId);
            if (it != SuspiciousNodesMarkTime_.end()) {
                nodeIdToSuspicionMarkTime[nodeId] = it->second;
            }
        }

        return nodeIdToSuspicionMarkTime;
    }

    bool ShouldMarkNodeSuspicious(const TError& error) const override
    {
        return IsSuspiciousNodeError(error);
    }

private:
    const NLogging::TLogger Logger;

    // TODO(akozhikhov): Add periodic to clear old suspicious nodes.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SuspiciousNodesSpinLock_);
    THashMap<TNodeId, TInstant> SuspiciousNodesMarkTime_;
};

INodeStatusDirectoryPtr CreateNodeStatusDirectory(NLogging::TLogger logger)
{
    return New<TNodeStatusDirectory>(std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
