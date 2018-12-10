#pragma once

#include "private.h"
#include "serialize.h"
#include "job_info.h"

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TDataBalancer
    : public TRefCounted
{
public:
    //! Used only for persistence.
    TDataBalancer() = default;

    TDataBalancer(
        TDataBalancerOptionsPtr options,
        i64 totalDataWeight,
        const NScheduler::TExecNodeDescriptorMap& execNodes);

    void Persist(const TPersistenceContext& context);

    void SetLogger(const NLogging::TLogger& logger);

    //! Account given data weight delta at the given node.
    void UpdateNodeDataWeight(const TJobNodeDescriptor& descriptor, i64 delta);

    //! Maybe remove some nodes from the active node set.
    void OnExecNodesUpdated(const NScheduler::TExecNodeDescriptorMap& newExecNodes);

    //! Check if scheduling a job with given data weight at the given node does not violate the balancing limits.
    bool CanScheduleJob(const TJobNodeDescriptor& descriptor, i64 dataWeight);

    //! Log a line of entries of form "nodeAddress[nodeId](+/-): dataWeight/dataWeightLimit" defining current data
    //! weight distribution and indicating currently active nodes.
    void LogStatistics() const;

private:
    TDataBalancerOptionsPtr Options_;

    struct TNode
    {
        i64 DataWeight = 0;
        //! Nodes is active if it was online during the whole operation.
        //! The total IO weight of such nodes is considered as what we have in a worst-case
        //! scenario, i.e. the data weight allowed for a node `x` is calculated
        //! as a `(x.IOWeight() / ActiveNodeTotalIOWeight_) * TotalDataWeight_ * Options_->Tolerance`.
        bool Active = false;
        TJobNodeDescriptor Descriptor;

        void Persist(const TPersistenceContext& context);
    };

    //! All nodes known to data balancer.
    THashMap<NNodeTrackerClient::TNodeId, TNode> IdToNode_;

    //! Total data weight to be distributed across nodes.
    i64 TotalDataWeight_ = 0;

    //! Total IO weight of all active nodes.
    i64 ActiveNodeTotalIOWeight_ = 0;

    TInstant LastLogTime_;
    int ConsecutiveViolationCount_ = 0;

    NLogging::TLogger Logger;

    TNode& GetOrRegisterNode(NNodeTrackerClient::TNodeId nodeId);
    TNode& GetOrRegisterNode(const TJobNodeDescriptor& descriptor);

    void LogViolation(const TNode& node, i64 dataWeight);

    i64 GetNodeDataWeightLimit(const TNode& node) const;
};

DEFINE_REFCOUNTED_TYPE(TDataBalancer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
