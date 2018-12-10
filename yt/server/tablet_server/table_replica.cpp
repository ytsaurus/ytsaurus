#include "table_replica.h"
#include "tablet.h"

#include <yt/server/table_server/replicated_table_node.h>

#include <yt/server/cell_master/serialize.h>

namespace NYT::NTabletServer {

using namespace NYPath;
using namespace NTableServer;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTableReplica::TTableReplica(const TTableReplicaId& id)
    : TObjectBase(id)
{ }

void TTableReplica::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, ClusterName_);
    Save(context, ReplicaPath_);
    Save(context, StartReplicationTimestamp_);
    Save(context, Table_);
    Save(context, State_);
    Save(context, Mode_);
    Save(context, TransitioningTablets_);
    Save(context, EnableReplicatedTableTracker_);
    Save(context, PreserveTimestamps_);
    Save(context, Atomicity_);
}

void TTableReplica::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, ClusterName_);
    Load(context, ReplicaPath_);
    Load(context, StartReplicationTimestamp_);
    Load(context, Table_);
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 602) {
        Load(context, Mode_);
    } else {
        Mode_ = ETableReplicaMode::Async;
    }
    Load(context, TransitioningTablets_);
    // COMPAT(aozeritsky)
    if (context.GetVersion() >= 717) {
        Load(context, EnableReplicatedTableTracker_);
    }
    if (context.GetVersion() >= 802) {
        Load(context, PreserveTimestamps_);
        Load(context, Atomicity_);
    }
}

void TTableReplica::ThrowInvalidState()
{
    THROW_ERROR_EXCEPTION("Table replica %v is in %Qlv state",
        Id_,
        State_);
}

TDuration TTableReplica::ComputeReplicationLagTime(TTimestamp latestTimestamp) const
{
    if (Mode_ == ETableReplicaMode::Sync) {
        return TDuration::Zero();
    }
    auto result = TDuration::Zero();
    for (auto* tablet : Table_->Tablets()) {
        const auto* replicaInfo = tablet->GetReplicaInfo(this);
        result = std::max(result, tablet->ComputeReplicationLagTime(latestTimestamp, *replicaInfo));
    }
    return result;
}

std::vector<TError> TTableReplica::GetErrors(std::optional<int> limit) const
{
    std::vector<TError> errors;
    errors.reserve(Table_->Tablets().size());
    for (auto* tablet : Table_->Tablets()) {
        const auto* replicaInfo = tablet->GetReplicaInfo(this);
        const auto& error = replicaInfo->Error();
        if (!error.IsOK()) {
            errors.push_back(error);
            if (limit && errors.size() >= *limit) {
                break;
            }
        }
    }
    return errors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

