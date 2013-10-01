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


    void StartFlush();
    void StopFlush();


    void OnModify(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    void OnAccess(
        TCypressNodeBase* trunkNode);


private:
    TCypressManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NProto::TMetaReqUpdateAccessStatistics UpdateAccessStatisticsRequest;
    std::vector<TCypressNodeBase*> NodesWithAccessStatisticsUpdate;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor;


    void Reset();

    void OnFlush();
    void OnCommitSucceeded();
    void OnCommitFailed(const TError& error);


    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
