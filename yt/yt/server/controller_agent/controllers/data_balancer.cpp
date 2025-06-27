#include "data_balancer.h"

#include "job_info.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TDataBalancer::TDataBalancer(
    TDataBalancerOptionsPtr options,
    i64 totalDataWeight,
    const TExecNodeDescriptorMap& execNodes,
    const NLogging::TLogger& logger)
    : Options_(std::move(options))
    , TotalDataWeight_(totalDataWeight)
    , Logger(logger)
{
    for (const auto& [nodeId, execNode] : execNodes) {
        auto& node = GetOrRegisterNode(execNode);
        if (auto weight = GetNodeIOWeight(node); weight > 0) {
            node.Active = true;
            ActiveNodeTotalIOWeight_ += weight;
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
    // NB: We do not add dataWeight here as it may block us when there is not so much data and lots of nodes.
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
    for (auto& [nodeId, node] : IdToNode_) {
        if (node.Active && !newExecNodes.contains(nodeId)) {
            node.Active = false;
            ActiveNodeTotalIOWeight_ -= GetNodeIOWeight(node);
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
    Persist(context, Logger);
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

void TDataBalancer::TNode::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DataWeight);
    Persist(context, Active);
    Persist(context, Descriptor);
}

void TDataBalancer::LogViolation(const TDataBalancer::TNode& node, i64 dataWeight)
{
    YT_LOG_DEBUG("Data balancing violation (NodeAddress: %v, NodeId: %v, DataWeight: %v, NodeDataWeight: %v, NodeLimit: %v)",
        NNodeTrackerClient::GetDefaultAddress(node.Descriptor.Addresses),
        node.Descriptor.Id,
        dataWeight,
        node.DataWeight,
        GetNodeDataWeightLimit(node));

    auto now = GetInstant();
    if (now > LastLogTime_ + Options_->LoggingPeriod) {
        LastLogTime_ = now;
        YT_LOG_WARNING("Too many data balancing violations (ConsecutiveViolationCount: %v, AdjustedDataWeightPerNode: %v)",
            ConsecutiveViolationCount_,
            DivCeil<i64>(TotalDataWeight_ * Options_->Tolerance, std::max<i64>(ActiveNodeTotalIOWeight_, 1)));
        LogStatistics();
    }
}

void TDataBalancer::LogStatistics() const
{
    TStringBuilder line;
    bool isFirst = true;
    for (const auto& [nodeId, node] : IdToNode_) {
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
            NNodeTrackerClient::GetDefaultAddress(node.Descriptor.Addresses),
            node.Descriptor.Id,
            isActive,
            node.DataWeight,
            GetNodeDataWeightLimit(node));
    }
    YT_LOG_DEBUG("Data balancer statistics (Statistics: {%v})", line.Flush());
}

i64 TDataBalancer::GetNodeDataWeightLimit(const TDataBalancer::TNode& node) const
{
    return DivCeil<i64>(
        TotalDataWeight_ * GetNodeIOWeight(node) * Options_->Tolerance,
        std::max<i64>(ActiveNodeTotalIOWeight_, 1));
}

double TDataBalancer::GetNodeIOWeight(const TNode& node) const
{
    if (Options_->UseNodeIOWeight) {
        return node.Descriptor.IOWeight;
    } else {
        return 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
