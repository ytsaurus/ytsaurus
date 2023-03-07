#include "table_replica.h"
#include "tablet.h"

#include <yt/server/master/table_server/replicated_table_node.h>

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NTabletServer {

using namespace NYPath;
using namespace NTableServer;
using namespace NTransactionClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTableReplica::TTableReplica(TTableReplicaId id)
    : TObject(id)
{ }

TString TTableReplica::GetLowercaseObjectName() const
{
    return Format("table replica %v", GetId());
}

TString TTableReplica::GetCapitalizedObjectName() const
{
    return Format("Table replica %v", GetId());
}

void TTableReplica::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

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
    TObject::Load(context);

    using NYT::Load;
    Load(context, ClusterName_);
    Load(context, ReplicaPath_);
    Load(context, StartReplicationTimestamp_);
    Load(context, Table_);
    Load(context, State_);
    Load(context, Mode_);
    Load(context, TransitioningTablets_);
    Load(context, EnableReplicatedTableTracker_);
    Load(context, PreserveTimestamps_);
    Load(context, Atomicity_);
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

int TTableReplica::GetErrorCount() const
{
    int errorCount = 0;
    for (auto* tablet : Table_->Tablets()) {
        const auto* replicaInfo = tablet->GetReplicaInfo(this);
        errorCount += replicaInfo->GetHasError();
    }
    return errorCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
