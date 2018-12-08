#include "data_balancer.h"

#include "config.h"

#include <yt/server/scheduler/exec_node.h>

#include <yt/server/controller_agent/job_info.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NControllerAgent {

using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TDataBalancer::TDataBalancer(
    TDataBalancerOptionsPtr options,
    i64 totalDataWeight,
    const TExecNodeDescriptorMap& execNodes)
    : Options_(std::move(options))
    , TotalDataWeight_(totalDataWeight)
{
    for (const auto& pair : execNodes) {
        const auto& execNode = pair.second;
        auto& node = GetOrRegisterNode(execNode);
        if (node.Descriptor.IOWeight > 0) {
            node.Active = true;
            ActiveNodeTotalIOWeight_ += node.Descriptor.IOWeight;
        }
    }
}

void TDataBalancer::UpdateNodeDataWeight(const TJobNodeDescriptor& descriptor, i64 delta)
{
    auto& node = GetOrRegisterNode(descriptor);
    node.DataWeight += delta;
}

bool TDataBalancer::CanScheduleJob(const TJobNodeDescriptor& descriptor, i64 dataWeight)
{
    auto& node = GetOrRegisterNode(descriptor);
    auto limit = GetNodeDataWeightLimit(node);
    // NB: we do not add dataWeight here as it may block us when there is not so much data and lots of nodes.
    if (node.DataWeight <= limit) {
        return true;
    } else {
        ++ConsecutiveViolationCount_;
        if (ConsecutiveViolationCount_ > Options_->LoggingMinConsecutiveViolationCount) {
            LogViolation(node, dataWeight);
        }
        return false;
    }
}

void TDataBalancer::OnExecNodesUpdated(const TExecNodeDescriptorMap& newExecNodes)
{
    for (auto& pair : IdToNode_) {
        const auto& nodeId = pair.first;
        auto& node = pair.second;
        if (node.Active && !newExecNodes.contains(nodeId)) {
            node.Active = false;
            ActiveNodeTotalIOWeight_ -= node.Descriptor.IOWeight;
        }
    }
}

void TDataBalancer::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Options_);
    Persist(context, TotalDataWeight_);
    Persist(context, IdToNode_);
    Persist(context, ActiveNodeTotalIOWeight_);
    Persist(context, ConsecutiveViolationCount_);
}

void TDataBalancer::SetLogger(const TLogger& logger)
{
    Logger = logger;
}

TDataBalancer::TNode& TDataBalancer::GetOrRegisterNode(NNodeTrackerClient::TNodeId nodeId)
{
    return IdToNode_[nodeId];
}

TDataBalancer::TNode& TDataBalancer::GetOrRegisterNode(const TJobNodeDescriptor& descriptor)
{
    auto nodeId = descriptor.Id;
    auto& node = GetOrRegisterNode(nodeId);
    node.Descriptor = descriptor;
    return node;
}

void TDataBalancer::TNode::Persist(const NYT::NTableClient::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DataWeight);
    Persist(context, Active);
    Persist(context, Descriptor);
}

void TDataBalancer::LogViolation(const TDataBalancer::TNode& node, i64 dataWeight)
{
    LOG_DEBUG("Data balancing violation (NodeAddress: %v, NodeId: %v, DataWeight: %v, NodeDataWeight: %v, NodeLimit: %v)",
        node.Descriptor.Address,
        node.Descriptor.Id,
        dataWeight,
        node.DataWeight,
        GetNodeDataWeightLimit(node));

    auto now = GetInstant();
    if (now > LastLogTime_ + Options_->LoggingPeriod) {
        LastLogTime_ = now;
        LOG_WARNING("Too many data balancing violations (ConsecutiveViolationCount: %v, AdjustedDataWeightPerNode: %v)",
            ConsecutiveViolationCount_,
            DivCeil<i64>(TotalDataWeight_ * Options_->Tolerance, std::max<i64>(ActiveNodeTotalIOWeight_, 1)));
        LogStatistics();
    }
}

void TDataBalancer::LogStatistics() const
{
    TStringBuilder line;
    bool isFirst = true;
    for (const auto& pair : IdToNode_) {
        auto nodeId = pair.first;
        auto& node = pair.second;
        if (!node.Active && node.DataWeight == 0) {
            // Nothing interesting in such a node.
            continue;
        }
        if (!isFirst) {
            line.AppendString(", ");
        }
        isFirst = false;
        char isActive = node.Active ? '+' : '-';
        line.AppendFormat("%v[%v]%v: %v/%v",
            node.Descriptor.Address,
            nodeId,
            isActive,
            node.DataWeight,
            GetNodeDataWeightLimit(node));
    }
    LOG_DEBUG("Data balancer statistics (Statistics: {%v})", line.Flush());
}

i64 TDataBalancer::GetNodeDataWeightLimit(const TDataBalancer::TNode& node) const
{
    return DivCeil<i64>(
        TotalDataWeight_ * node.Descriptor.IOWeight * Options_->Tolerance,
        std::max<i64>(ActiveNodeTotalIOWeight_, 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
