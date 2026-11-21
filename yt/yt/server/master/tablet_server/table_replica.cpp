#include "table_replica.h"
#include "tablet.h"

#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NYPath;
using namespace NTableServer;
using namespace NTransactionClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

std::string TTableReplica::GetLowercaseObjectName() const
{
    return Format("table replica %v", GetId());
}

std::string TTableReplica::GetCapitalizedObjectName() const
{
    return Format("Table replica %v", GetId());
}

void TTableReplica::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, ClusterName_);
    PHOENIX_REGISTER_FIELD(2, ReplicaPath_);
    PHOENIX_REGISTER_FIELD(3, StartReplicationTimestamp_);
    PHOENIX_REGISTER_FIELD(4, Table_);
    PHOENIX_REGISTER_FIELD(5, State_);
    PHOENIX_REGISTER_FIELD(6, Mode_);
    PHOENIX_REGISTER_FIELD(7, TransitioningTablets_);
    PHOENIX_REGISTER_FIELD(8, EnableReplicatedTableTracker_);
    PHOENIX_REGISTER_FIELD(9, PreserveTimestamps_);
    PHOENIX_REGISTER_FIELD(10, Atomicity_);
}

PHOENIX_DEFINE_TYPE(TTableReplica);

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
    for (auto tablet : Table_->Tablets()) {
        const auto* replicaInfo = tablet->As<TTablet>()->GetReplicaInfo(this);
        result = std::max(result, tablet->As<TTablet>()->ComputeReplicationLagTime(
            latestTimestamp,
            *replicaInfo,
            StartReplicationTimestamp_));
    }
    return result;
}

int TTableReplica::GetErrorCount() const
{
    int errorCount = 0;
    for (auto tablet : Table_->Tablets()) {
        const auto* replicaInfo = tablet->As<TTablet>()->GetReplicaInfo(this);
        errorCount += replicaInfo->GetHasError();
    }
    return errorCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
