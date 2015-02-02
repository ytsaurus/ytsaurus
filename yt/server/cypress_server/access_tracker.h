#pragma once

#include "public.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>

#include <core/misc/error.h>

#include <server/cell_master/public.h>

#include <server/cypress_server/cypress_manager.pb.h>

#include <server/transaction_server/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TAccessTracker
    : public TRefCounted
{
public:
    explicit TAccessTracker(
        TCypressManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);


    void Start();
    void Stop();


    void OnModify(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    void OnAccess(
        TCypressNodeBase* trunkNode);


private:
    TCypressManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;

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
