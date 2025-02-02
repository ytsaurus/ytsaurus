#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTableReplica
    : public NObjectServer::TObject
    , public TRefTracked<TTableReplica>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, ClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, ReplicaPath);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, StartReplicationTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TReplicatedTableNodeRawPtr, Table);
    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaState, State, ETableReplicaState::None);
    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaMode, Mode, ETableReplicaMode::Async);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletRawPtr>, TransitioningTablets);
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableReplicatedTableTracker, true);
    DEFINE_BYVAL_RW_PROPERTY(bool, PreserveTimestamps, true);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::EAtomicity, Atomicity, NTransactionClient::EAtomicity::Full);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void ThrowInvalidState();

    TDuration ComputeReplicationLagTime(NTransactionClient::TTimestamp latestTimestamp) const;
    int GetErrorCount() const;
};

DEFINE_MASTER_OBJECT_TYPE(TTableReplica)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

