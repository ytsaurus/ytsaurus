#include "test_coordination_helper.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

using namespace NActors;
using namespace NYql;

namespace {

class TMockLock: public TActor<TMockLock> {
public:
    TMockLock()
        : TActor(&TMockLock::Main)
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

class TProbeLock: public TActor<TProbeLock> {
public:
    explicit TProbeLock(TIntrusivePtr<TLockProbe> probe)
        : TActor(&TProbeLock::Main)
        , Probe_(std::move(probe))
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBecomeFollower, OnBecomeFollower);
            cFunc(TEvents::TEvPoison::EventType, OnPoison);
        }
    }

    void OnBecomeFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
        ++Probe_->BecomeFollowerCount;
    }

    void OnPoison() {
        ++Probe_->PoisonCount;
        PassAway();
    }

    TIntrusivePtr<TLockProbe> Probe_;
};

} // namespace

TTestCoordinationHelper::TTestCoordinationHelper(ui32 nodeId, TIntrusivePtr<TLockProbe> lockProbe)
    : NodeId_(nodeId)
    , LockProbe_(std::move(lockProbe))
{
}

ui32 TTestCoordinationHelper::GetNodeId() {
    return NodeId_;
}

ui32 TTestCoordinationHelper::GetNodeId(
    const TMaybe<ui32> nodeId,
    const TMaybe<TString>& grpcPort,
    ui32 minNodeId,
    ui32 maxNodeId,
    const THashMap<TString, TString>& attributes)
{
    Y_UNUSED(nodeId);
    Y_UNUSED(grpcPort);
    Y_UNUSED(minNodeId);
    Y_UNUSED(maxNodeId);
    Y_UNUSED(attributes);
    return NodeId_;
}

TString TTestCoordinationHelper::GetHostname() {
    return "localhost";
}

TString TTestCoordinationHelper::GetIp() {
    return "::1";
}

NActors::IActor* TTestCoordinationHelper::CreateLockOnCluster(
    NActors::TActorId ytWrapper,
    const TString& prefix,
    const TString& lockName,
    bool temporary)
{
    Y_UNUSED(ytWrapper);
    Y_UNUSED(prefix);
    return CreateLock(lockName, temporary);
}

NActors::IActor* TTestCoordinationHelper::CreateLock(const TString& lockName, bool temporary) {
    Y_UNUSED(lockName);
    Y_UNUSED(temporary);
    ++CreateLockCalls;
    if (LockProbe_) {
        return new TProbeLock(LockProbe_);
    }
    return new TMockLock();
}

NActors::IActor* TTestCoordinationHelper::CreateServiceNodePinger(
    const IServiceNodeResolver::TPtr& ptr,
    const TResourceManagerOptions& rmOptions,
    const THashMap<TString, TString>& attributes)
{
    Y_UNUSED(ptr);
    Y_UNUSED(rmOptions);
    Y_UNUSED(attributes);
    return new TMockLock();
}

void TTestCoordinationHelper::StartRegistrator(NActors::TActorSystem* actorSystem) {
    Y_UNUSED(actorSystem);
}

void TTestCoordinationHelper::StartGlobalWorker(
    NActors::TActorSystem* actorSystem,
    const TVector<TResourceManagerOptions>& resourceUploaderOptions,
    IMetricsRegistryPtr metricsRegistry)
{
    Y_UNUSED(actorSystem);
    Y_UNUSED(resourceUploaderOptions);
    Y_UNUSED(metricsRegistry);
}

void TTestCoordinationHelper::StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) {
    Y_UNUSED(actorSystem);
    Y_UNUSED(role);
}

IServiceNodeResolver::TPtr TTestCoordinationHelper::CreateServiceNodeResolver(
    NActors::TActorSystem* actorSystem,
    const TVector<TString>& hostPortPairs)
{
    Y_UNUSED(actorSystem);
    Y_UNUSED(hostPortPairs);
    return nullptr;
}

const NProto::TDqConfig::TYtCoordinator& TTestCoordinationHelper::GetConfig() {
    return YtCoordinator_;
}

NActors::TActorId TTestCoordinationHelper::GetWrapper(NActors::TActorSystem* actorSystem) {
    Y_UNUSED(actorSystem);
    return NActors::TActorId();
}

NActors::TActorId TTestCoordinationHelper::GetWrapper(
    NActors::TActorSystem* actorSystem,
    const TString& cluster,
    const TString& user,
    const TString& token)
{
    Y_UNUSED(actorSystem);
    Y_UNUSED(cluster);
    Y_UNUSED(user);
    Y_UNUSED(token);
    return NActors::TActorId();
}

NActors::TActorId TTestCoordinationHelper::GetWrapper() {
    return NActors::TActorId();
}

TWorkerRuntimeData* TTestCoordinationHelper::GetRuntimeData() {
    return nullptr;
}

void TTestCoordinationHelper::Stop(NActors::TActorSystem* actorSystem) {
    Y_UNUSED(actorSystem);
}

TString TTestCoordinationHelper::GetRevision() {
    return TString();
}
