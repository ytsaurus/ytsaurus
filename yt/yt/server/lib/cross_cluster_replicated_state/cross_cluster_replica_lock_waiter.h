#pragma once

#include "cross_cluster_client.h"
#include "cross_cluster_replica_version.h"

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

enum class ELockAcquisionState
{
    Acquired,
    Outdated,
};

////////////////////////////////////////////////////////////////////////////////

class ICrossClusterReplicaLockWaiter
    : public TRefCounted
{
public:
    virtual void Start() = 0;

    virtual TFuture<void> Stop() = 0;

    virtual TFuture<ELockAcquisionState> WaitForLockAcquision(
        NCypressClient::TLockId lockId,
        ISingleClusterClientPtr client,
        NApi::ITransactionPtr transaction,
        NYPath::TYPath nodePath,
        TReplicaVersion targetVersion,
        TDuration timeout) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICrossClusterReplicaLockWaiter);

ICrossClusterReplicaLockWaiterPtr CreateCrossClusterReplicaLockWaiter(
    IInvokerPtr invoker, TCrossClusterReplicatedStateConfigPtr config, NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NCrossClusterReplicatedState
