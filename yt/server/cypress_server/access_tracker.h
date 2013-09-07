#pragma once

#include "public.h"

#include <ytlib/concurrency/periodic_invoker.h>
#include <ytlib/concurrency/thread_affinity.h>

#include <ytlib/misc/error.h>

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


    void SetModified(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    void SetAccessed(
        TCypressNodeBase* trunkNode);


private:
    TCypressManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NProto::TMetaReqUpdateAccessStatistics UpdateAccessStatisticsRequest;
    std::vector<TCypressNodeBase*> NodesWithAccessStatisticsUpdate;

    TPeriodicInvokerPtr FlushInvoker;


    void Reset();

    void OnFlush();
    void OnCommitSucceeded();
    void OnCommitFailed(const TError& error);


    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
