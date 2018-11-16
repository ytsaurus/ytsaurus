#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/cypress_manager.pb.h>

#include <yt/server/transaction_server/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TAccessTracker
    : public TRefCounted
{
public:
    TAccessTracker(
        TCypressManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void SetModified(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        EModificationType modificationType);

    void SetAccessed(TCypressNodeBase* trunkNode);

private:
    const TCypressManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    NProto::TReqUpdateAccessStatistics UpdateAccessStatisticsRequest_;
    std::vector<TCypressNodeBase*> NodesWithAccessStatisticsUpdate_;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void Reset();
    void OnFlush();

};

DEFINE_REFCOUNTED_TYPE(TAccessTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
