#include "incumbent_manager.h"

#include "private.h"
#include "config.h"
#include "incumbent.h"
#include "scheduler.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/lib/incumbent_client/incumbent_service_proxy.h>
#include <yt/yt/server/lib/incumbent_client/proto/incumbent_service.pb.h>

#include <yt/yt/ytlib/incumbent_client/incumbent_descriptor.h>

#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NIncumbentServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NIncumbentClient;
using namespace NLogging;
using namespace NHydra;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = IncumbentServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TIncumbentManager
    : public IIncumbentManager
    , public TMasterAutomatonPart
{
public:
    explicit TIncumbentManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::IncumbentManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TIncumbentManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TIncumbentManager::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkManager));
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        BuildYsonFluently(consumer).BeginMap()
            .Item("local_state").Value(LocalIncumbentMap_)
            .DoIf(IsLeader(), [&] (TFluentMap map) {
                map
                    .Item("current_state").Value(CurrentIncumbentMap_)
                    .Item("target_state").Value(TargetIncumbentMap_);
            })
        .EndMap();
    }

    void RegisterIncumbent(IIncumbentPtr incumbent) override
    {
        auto type = incumbent->GetType();
        YT_VERIFY(!Incumbents_[type]);
        Incumbents_[type] = std::move(incumbent);
    }

    int GetIncumbentCount(EIncumbentType type) const override
    {
        TCompactSet<TString, 7> addresses;
        for (auto& address : LocalIncumbentMap_[type].Addresses) {
            if (address) {
                addresses.insert(*address);
            }
        }
        return std::ssize(addresses);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TIncumbentManagerConfigPtr Config_;

    // Leader state.
    struct TPeerState
    {
        TString Address;

        bool Leader = false;
        bool Online = false;

        TIncumbentMap ReportedIncumbents = CreateEmptyIncumbentMap();

        TLease Lease;

        TPeerDescriptor ToPeerDescriptor() const
        {
            EPeerState state = EPeerState::None;
            if (Leader) {
                YT_VERIFY(Online);
                state = EPeerState::Leading;
            } else if (Online) {
                state = EPeerState::Following;
            }

            return TPeerDescriptor{
                .Address = Address,
                .State = state,
            };
        }
    };
    std::vector<TPeerState> Peers_;

    TIncumbentMap CurrentIncumbentMap_ = CreateEmptyIncumbentMap();
    TIncumbentMap TargetIncumbentMap_ = CreateEmptyIncumbentMap();

    TPeriodicExecutorPtr AssignExecutor_;

    bool CanScheduleIncumbents_ = false;
    TInstant EpochStart_ = TInstant::Now();

    // Peer state.

    TIncumbentMap LocalIncumbentMap_ = CreateEmptyIncumbentMap();

    TLease LocalLease_;

    TEnumIndexedVector<EIncumbentType, IIncumbentPtr> Incumbents_;

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Config_ = Bootstrap_->GetConfigManager()->GetConfig()->IncumbentManager;

        // Postprocessor + DefaultNew = fail.
        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            auto& config = Config_->Scheduler->Incumbents[incumbentType];
            if (!config) {
                config = New<TIncumbentSchedulingConfig>();
            }
        }

        if (AssignExecutor_) {
            AssignExecutor_->SetPeriod(Config_->AssignPeriod);
        }
    }

    TString GetSelfAddress() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& cellManager = Bootstrap_->GetCellManager();
        return *cellManager->GetSelfConfig().Address;
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        TMasterAutomatonPart::OnLeaderActive();

        OnStartEpoch();

        YT_VERIFY(Peers_.empty());
        const auto& cellManager = Bootstrap_->GetCellManager();
        int peerCount = cellManager->GetTotalPeerCount();
        Peers_.reserve(peerCount);

        for (int peerIndex = 0; peerIndex < peerCount; ++peerIndex) {
            bool leader = peerIndex == cellManager->GetSelfPeerId();
            Peers_.push_back(TPeerState{
                .Address = *cellManager->GetPeerConfig(peerIndex).Address,
                .Leader = leader,
                .Online = leader,
            });
        }

        CanScheduleIncumbents_ = false;
        EpochStart_ = TInstant::Now();

        CurrentIncumbentMap_ = CreateEmptyIncumbentMap();

        UpdateTargetState();

        AssignExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND(&TIncumbentManager::AssignIncumbents, MakeWeak(this)),
            Config_->AssignPeriod);
        AssignExecutor_->Start();
    }

    void OnStartFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartFollowing();

        OnStartEpoch();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        Peers_.clear();

        if (AssignExecutor_) {
            YT_UNUSED_FUTURE(AssignExecutor_->Stop());
            AssignExecutor_.Reset();
        }

        TargetIncumbentMap_ = CreateEmptyIncumbentMap();

        OnEndEpoch();
    }

    void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();

        OnEndEpoch();
    }

    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& descriptor : LocalIncumbentMap_) {
            for (const auto& address : descriptor.Addresses) {
                YT_VERIFY(!address);
            }
        }
    }

    void OnEndEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TLeaseManager::CloseLease(LocalLease_);
        LocalLease_.Reset();

        AbandonLocalIncumbencies();
    }

    void OnHeartbeat(
        TInstant peerLeaseDeadline,
        const TIncumbentMap& incumbentMap) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto selfAddress = GetSelfAddress();

        YT_LOG_DEBUG("Received heartbeat from leader "
            "(PeerLeaseDeadline: %v, SelfAddress: %v)",
            peerLeaseDeadline,
            selfAddress);

        if (Config_->BannedPeers.contains(selfAddress)) {
            THROW_ERROR_EXCEPTION("Peer is banned");
        }

        auto now = TInstant::Now();
        if (peerLeaseDeadline < now) {
            if (LocalLease_) {
                TLeaseManager::CloseLease(LocalLease_);
            }

            OnLocalLeaseExpired();
        } else {
            if (LocalLease_) {
                TLeaseManager::RenewLease(LocalLease_, peerLeaseDeadline - now);
            } else {
                LocalLease_ = TLeaseManager::CreateLease(
                    peerLeaseDeadline - now,
                    BIND(&TIncumbentManager::OnLocalLeaseExpired, MakeWeak(this))
                        .Via(EpochAutomatonInvoker_));
            }
        }

        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            auto& currentAddresses = LocalIncumbentMap_[incumbentType].Addresses;
            const auto& newAddresses = incumbentMap[incumbentType].Addresses;
            YT_VERIFY(std::ssize(currentAddresses) == GetIncumbentShardCount(incumbentType));
            YT_VERIFY(std::ssize(newAddresses) == GetIncumbentShardCount(incumbentType));

            for (int shardIndex = 0; shardIndex < std::ssize(currentAddresses); ++shardIndex) {
                auto& currentAddress = currentAddresses[shardIndex];
                const auto& newAddress = newAddresses[shardIndex];
                if (currentAddress == newAddress) {
                    continue;
                }

                YT_LOG_DEBUG("Updating incumbent address (Type: %v, ShardIndex: %v, Address: %v -> %v)",
                    incumbentType,
                    shardIndex,
                    currentAddress,
                    newAddress);

                if (currentAddress == selfAddress) {
                    OnIncumbencyFinished(TIncumbencyDescriptor{
                        .Type = incumbentType,
                        .ShardIndex = shardIndex
                    });
                }
                currentAddress = {};

                currentAddress = newAddress;
                if (currentAddress == selfAddress) {
                    OnIncumbencyStarted(TIncumbencyDescriptor{
                        .Type = incumbentType,
                        .ShardIndex = shardIndex
                    });
                }
            }
        }
    }

    bool HasIncumbency(EIncumbentType type, int shardIndex) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return LocalIncumbentMap_[type].Addresses[shardIndex] == GetSelfAddress();
    }

    std::optional<TString> GetIncumbentAddress(EIncumbentType type, int shardIndex) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return LocalIncumbentMap_[type].Addresses[shardIndex];
    }

    void AssignIncumbents()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CanScheduleIncumbents_) {
            auto allPeersOnline = true;
            for (const auto& peer : Peers_) {
                if (!peer.Online) {
                    allPeersOnline = false;
                    break;
                }
            }

            if (EpochStart_ + Config_->PeerGracePeriod < TInstant::Now() || allPeersOnline) {
                CanScheduleIncumbents_ = true;
            }
        }

        YT_LOG_DEBUG("Started incumbents assign iteration "
            "(CanScheduleIncumbents: %v)",
            CanScheduleIncumbents_);

        UpdateTargetState();

        std::vector<TFuture<void>> peerResponseFutures;
        peerResponseFutures.reserve(Peers_.size());

        for (int peerIndex = 0; peerIndex < std::ssize(Peers_); ++peerIndex) {
            TIncumbentMap newIncumbents;
            for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
                auto shardCount = GetIncumbentShardCount(incumbentType);
                const auto& currentAddresses = TargetIncumbentMap_[incumbentType].Addresses;
                const auto& targetAddresses = TargetIncumbentMap_[incumbentType].Addresses;
                YT_VERIFY(std::ssize(currentAddresses) == shardCount);
                YT_VERIFY(std::ssize(targetAddresses) == shardCount);

                auto& newDescriptor = newIncumbents[incumbentType];
                auto& newAddresses = newDescriptor.Addresses;
                newAddresses.resize(shardCount);

                for (int shardIndex = 0; shardIndex < std::ssize(targetAddresses); ++shardIndex) {
                    const auto& currentAddress = currentAddresses[shardIndex];
                    const auto& targetAddress = targetAddresses[shardIndex];
                    if (currentAddress == targetAddress || (!currentAddress && CanScheduleIncumbents_)) {
                        newAddresses[shardIndex] = targetAddress;
                    }
                }
            }

            const auto& cellManager = Bootstrap_->GetCellManager();
            auto peerChannel = cellManager->GetPeerChannel(peerIndex);

            TIncumbentServiceProxy proxy(peerChannel);
            auto heartbeat = proxy.Heartbeat();
            heartbeat->SetTimeout(Config_->HeartbeatTimeout);

            for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
                const auto& descriptor = newIncumbents[incumbentType];
                auto* protoDescriptor = heartbeat->add_descriptors();
                protoDescriptor->set_type(static_cast<int>(incumbentType));
                for (const auto& address : descriptor.Addresses) {
                    if (address) {
                        protoDescriptor->add_addresses(*address);
                    } else {
                        protoDescriptor->add_addresses();
                    }
                }
            }

            auto requestStart = TInstant::Now();
            heartbeat->set_peer_lease_deadline(ToProto<i64>(requestStart + Config_->PeerLeaseDuration));

            auto future = heartbeat->Invoke()
                .Apply(BIND(
                    &TIncumbentManager::OnHeartbeatReported,
                    MakeWeak(this),
                    peerIndex,
                    requestStart,
                    newIncumbents)
                    .Via(EpochAutomatonInvoker_));
            peerResponseFutures.push_back(future);
        }

        WaitFor(AllSet(std::move(peerResponseFutures)))
            .ThrowOnError();

        YT_LOG_DEBUG("Incumbents assign iteration completed");
    }

    void UpdateTargetState()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        std::vector<TPeerDescriptor> peerDescriptors;
        peerDescriptors.reserve(Peers_.size());
        for (int peerIndex = 0; peerIndex < std::ssize(Peers_); ++peerIndex) {
            peerDescriptors.push_back(Peers_[peerIndex].ToPeerDescriptor());
        }

        auto assignment = ScheduleIncumbents(Config_->Scheduler, peerDescriptors);
        TargetIncumbentMap_ = std::move(assignment);
    }

    void OnHeartbeatReported(
        int peerIndex,
        TInstant requestStart,
        TIncumbentMap reportedIncumbents,
        const TErrorOr<TIncumbentServiceProxy::TRspHeartbeatPtr>& response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto& peer = Peers_[peerIndex];

        if (response.IsOK()) {
            OnPeerOnline(peerIndex);

            for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
                for (int shardIndex = 0; shardIndex < GetIncumbentShardCount(incumbentType); ++shardIndex) {
                    const auto& previousAddress = peer.ReportedIncumbents[incumbentType].Addresses[shardIndex];
                    const auto& reportedAddress = reportedIncumbents[incumbentType].Addresses[shardIndex];
                    if (reportedAddress == peer.Address) {
                        CurrentIncumbentMap_[incumbentType].Addresses[shardIndex] = peer.Address;
                    } else if (previousAddress == peer.Address && !reportedAddress) {
                        CurrentIncumbentMap_[incumbentType].Addresses[shardIndex] = {};
                    }
                }
            }

            peer.ReportedIncumbents = std::move(reportedIncumbents);

            auto peerLeaseDeadline = requestStart + Config_->PeerGracePeriod;
            auto now = TInstant::Now();
            if (peerLeaseDeadline < now) {
                if (peer.Lease) {
                    TLeaseManager::CloseLease(peer.Lease);
                }

                OnPeerLeaseExpired(peerIndex);
            } else {
                if (peer.Lease) {
                    TLeaseManager::RenewLease(peer.Lease, peerLeaseDeadline - now);
                } else {
                    peer.Lease = TLeaseManager::CreateLease(
                        peerLeaseDeadline - now,
                        BIND(&TIncumbentManager::OnPeerLeaseExpired, MakeWeak(this), peerIndex)
                            .Via(EpochAutomatonInvoker_));
                }
            }

            YT_LOG_DEBUG("Successfully reported heartbeat to peer (PeerIndex: %v, PeerAddress: %v)",
                peerIndex,
                peer.Address);
        } else {
            YT_LOG_WARNING(response, "Failed to report heartbeat to peer (PeerIndex: %v, PeerAddress: %v)",
                peerIndex,
                peer.Address);

            // Failure does not imply that heartbeat was not reported to peer, so we
            // pessimistically assume that assignments were successful and reovacations failed.
            for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
                for (int shardIndex = 0; shardIndex < GetIncumbentShardCount(incumbentType); ++shardIndex) {
                    if (reportedIncumbents[incumbentType].Addresses[shardIndex] == peer.Address) {
                        CurrentIncumbentMap_[incumbentType].Addresses[shardIndex] = peer.Address;
                        peer.ReportedIncumbents[incumbentType].Addresses[shardIndex] = peer.Address;
                    }
                }
            }
        }
    }

    void OnPeerOnline(int peerIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto& peer = Peers_[peerIndex];
        if (peer.Leader) {
            return;
        }

        if (peer.Online) {
            return;
        }

        peer.Online = true;

        YT_LOG_INFO("Peer is online (PeerIndex: %v, PeerAddress: %v)",
            peerIndex,
            peer.Address);
    }

    void OnPeerLeaseExpired(int peerIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto& peer = Peers_[peerIndex];
        if (peer.Leader) {
            return;
        }

        if (!peer.Online) {
            return;
        }

        peer.Online = false;
        peer.Lease.Reset();

        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            for (auto& address : CurrentIncumbentMap_[incumbentType].Addresses) {
                if (address == peer.Address) {
                    address = {};
                }
            }
        }

        YT_LOG_INFO("Peer is offline (PeerIndex: %v, PeerAddress: %v)",
            peerIndex,
            peer.Address);
    }

    void OnIncumbencyStarted(TIncumbencyDescriptor incumbency)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (const auto& incumbent = Incumbents_[incumbency.Type]) {
            incumbent->OnIncumbencyStarted(incumbency.ShardIndex);
        }

        YT_LOG_INFO("Incumbency started (Type: %v, ShardIndex: %v)",
            incumbency.Type,
            incumbency.ShardIndex);
    }

    void OnIncumbencyFinished(TIncumbencyDescriptor incumbency)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto& incumbencyAddress = LocalIncumbentMap_[incumbency.Type].Addresses[incumbency.ShardIndex];
        YT_LOG_ALERT_UNLESS(incumbencyAddress == GetSelfAddress(),
            "Local incumbency finished but it is assigned to another peer according to local state "
            "(SelfAddress: %v, IncumbencyAddress: %v)",
            GetSelfAddress(),
            incumbencyAddress);

        incumbencyAddress = {};

        if (const auto& incumbent = Incumbents_[incumbency.Type]) {
            incumbent->OnIncumbencyFinished(incumbency.ShardIndex);
        }

        YT_LOG_INFO("Incumbency finished (Type: %v, ShardIndex: %v)",
            incumbency.Type,
            incumbency.ShardIndex);
    }

    void OnLocalLeaseExpired()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_WARNING("Local lease expired, abandoning all local incumbencies");

        AbandonLocalIncumbencies();
    }

    void AbandonLocalIncumbencies()
    {
        auto selfAddress = GetSelfAddress();
        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            for (int shardIndex = 0; shardIndex < GetIncumbentShardCount(incumbentType); ++shardIndex) {
                if (HasIncumbency(incumbentType, shardIndex)) {
                    OnIncumbencyFinished(TIncumbencyDescriptor{
                        .Type = incumbentType,
                        .ShardIndex = shardIndex,
                    });
                }
            }
        }
        LocalIncumbentMap_ = CreateEmptyIncumbentMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IIncumbentManagerPtr CreateIncumbentManager(TBootstrap* bootstrap)
{
    return New<TIncumbentManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
