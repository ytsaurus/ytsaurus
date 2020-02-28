#pragma once

#include "public.h"

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/table_server/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/enum.h>

#include <yt/core/ypath/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTableReplica
    : public NObjectServer::TObject
    , public TRefTracked<TTableReplica>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, ClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, ReplicaPath);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, StartReplicationTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TReplicatedTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaState, State, ETableReplicaState::None);
    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaMode, Mode, ETableReplicaMode::Async)
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTablet*>, TransitioningTablets)
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableReplicatedTableTracker, true)
    DEFINE_BYVAL_RW_PROPERTY(bool, PreserveTimestamps, true)
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::EAtomicity, Atomicity, NTransactionClient::EAtomicity::Full)

public:
    explicit TTableReplica(TTableReplicaId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void ThrowInvalidState();

    TDuration ComputeReplicationLagTime(NTransactionClient::TTimestamp latestTimestamp) const;
    int GetErrorCount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

