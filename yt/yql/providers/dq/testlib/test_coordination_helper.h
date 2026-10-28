#pragma once

#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TLockProbe: TThrRefBase {
    ui32 BecomeFollowerCount = 0;
    ui32 PoisonCount = 0;
};

class TTestCoordinationHelper: public ICoordinationHelper {
public:
    explicit TTestCoordinationHelper(ui32 nodeId, TIntrusivePtr<TLockProbe> lockProbe = nullptr);

    ui32 GetNodeId() override;
    ui32 GetNodeId(
        const TMaybe<ui32> nodeId,
        const TMaybe<TString>& grpcPort,
        ui32 minNodeId,
        ui32 maxNodeId,
        const THashMap<TString, TString>& attributes) override;

    TString GetHostname() override;
    TString GetIp() override;

    NActors::IActor* CreateLockOnCluster(
        NActors::TActorId ytWrapper,
        const TString& prefix,
        const TString& lockName,
        bool temporary) override;

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override;

    NActors::IActor* CreateServiceNodePinger(
        const IServiceNodeResolver::TPtr& ptr,
        const TResourceManagerOptions& rmOptions,
        const THashMap<TString, TString>& attributes = {}) override;

    void StartRegistrator(NActors::TActorSystem* actorSystem) override;
    void StartGlobalWorker(
        NActors::TActorSystem* actorSystem,
        const TVector<TResourceManagerOptions>& resourceUploaderOptions,
        IMetricsRegistryPtr metricsRegistry) override;
    void StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) override;

    IServiceNodeResolver::TPtr CreateServiceNodeResolver(
        NActors::TActorSystem* actorSystem,
        const TVector<TString>& hostPortPairs) override;

    const NProto::TDqConfig::TYtCoordinator& GetConfig() override;

    NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem) override;
    NActors::TActorId GetWrapper(
        NActors::TActorSystem* actorSystem,
        const TString& cluster,
        const TString& user,
        const TString& token) override;
    NActors::TActorId GetWrapper() override;

    TWorkerRuntimeData* GetRuntimeData() override;
    void Stop(NActors::TActorSystem* actorSystem) override;
    TString GetRevision() override;

    ui32 CreateLockCalls = 0;

private:
    ui32 NodeId_;
    TIntrusivePtr<TLockProbe> LockProbe_;
    NProto::TDqConfig::TYtCoordinator YtCoordinator_;
};

} // namespace NYql
