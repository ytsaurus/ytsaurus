#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TClusterClockConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TClusterClockConfigPtr& GetConfig() const;

    NObjectClient::TCellId GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NObjectClient::TCellTag GetCellTag() const;

    const NRpc::IServerPtr& GetRpcServer() const;
    const NRpc::IChannelPtr& GetLocalRpcChannel() const;
    const NElection::TCellManagerPtr& GetCellManager() const;
    const NHydra::IChangelogStoreFactoryPtr& GetChangelogStoreFactory() const;
    const NHydra::ISnapshotStorePtr& GetSnapshotStore() const;
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const;
    const THydraFacadePtr& GetHydraFacade() const;

    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetSnapshotIOInvoker() const;

    void Initialize();
    void Run();
    void TryLoadSnapshot(const TString& fileName, bool dump);

private:
    const TClusterClockConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NObjectClient::TCellId CellId_;
    NObjectClient::TCellTag CellTag_;

    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;
    THydraFacadePtr HydraFacade_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    NConcurrency::TActionQueuePtr SnapshotIOQueue_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    void DoInitialize();
    void DoRun();
    void DoLoadSnapshot(const TString& fileName, bool dump);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
