#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/library/fusion/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public NServer::IDaemonBootstrap
{
public:
    TBootstrap(
        TClusterClockBootstrapConfigPtr config,
        NYTree::INodePtr configNode,
        NFusion::IServiceLocatorPtr serviceLocator);
    ~TBootstrap();

    const TClusterClockBootstrapConfigPtr& GetConfig() const;

    NObjectClient::TCellId GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NObjectClient::TCellTag GetCellTag() const;

    const NRpc::IServerPtr& GetRpcServer() const;
    const NRpc::IChannelPtr& GetLocalRpcChannel() const;
    const NElection::TCellManagerPtr& GetCellManager() const;
    const NHydra::IChangelogStoreFactoryPtr& GetChangelogStoreFactory() const;
    const NHydra::ISnapshotStorePtr& GetSnapshotStore() const;
    const THydraFacadePtr& GetHydraFacade() const;

    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetSnapshotIOInvoker() const;

    TFuture<void> Run() final;

    void Initialize();
    void LoadSnapshot(const TString& fileName, bool dump);

private:
    const TClusterClockBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const NFusion::IServiceLocatorPtr ServiceLocator_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::TActionQueuePtr SnapshotIOQueue_;

    NObjectClient::TCellId CellId_;
    NObjectClient::TCellTag CellTag_;

    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    THydraFacadePtr HydraFacade_;


    void DoRun();
    void DoInitialize();
    void DoStart();

    void DoLoadSnapshot(const TString& fileName, bool dump);

    NYTree::IYPathServicePtr CreateCellOrchidService() const;
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateClusterClockBootstrap(
    TClusterClockBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
