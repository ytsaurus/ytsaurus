#include "smooth_movement_tracker.h"

#include "automaton.h"
#include "config.h"
#include "store_manager.h"
#include "tablet.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/persistent_mailbox_state_cookie.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

/*!
    Smooth movement tracker guides tablets through various smooth movement stages.
  The journey of each tablet is mostly linear with small variations. Stage can
  be changed by one of the following signals:
    - internal decision of the tablet (e.g. all transactions have finished);
    - Hive message from the sibling servant.

    Internal decisions are made by the |CheckTablet| method which is called whenever
  something has been done to the tablet. It may be called both in or out of the mutation
  context. If stage should be changed then the appropriate mutation is scheduled
  and the corresponding flag |StageChangeScheduled| is set. Hive stage change requests
  are applied immediately. Note that all stages are persistent, while |StageChangeScheduled|
  flag is transient.

    Tablet can participate in one of two roles, source or target. Roles never change
  during a certain movement process (of course, when one movement has finished
  its target may later participate in another movement as source).

    Initially, target is mounted with TargetAllocated stage and source transits to
  TargetAllocated stage upon |TReqStartSmoothMovement| message from master. Further
  transition descriptions follow.

    Legend of the first column:
  R - reads, W - writes, C - compactions (tablet stores update)
  S - allowed at source, T - allowed at target, - - allowed at neither


  RWC  SOURCE                                       TARGET

  SSS
  SS-  (*) TargetAllocated                          (*) TargetAllocated
  SS-   *  * tablet stores update is forbidden       *  * no reads, no writes
  SS-   *                                            *
  SS-   *  - wait until no tablet store update       *
  SS-   *    is prepared                             *
  SS-   *                                            *
  S--  (*) WaitingForLocksBeforeActivation           *
  S--   *  * write to tablet is forbidden            *
  S--   *                                            *
  S--   *  - wait until all persistent and           *
  S--   *  - transient transactions finish           *
  S--   *                                            *
  SSS  (*) TargetActivated                           *
  SSS   *  * tablet stores update is allowed         *
  SSS   *                                            *
  SSS   *        TReqReplicateTabletContent          *
  SSS   * ----------------------------------------> (*) TargetActivated
  SSS   *                                            *  * tablet stores update is forbidden
  SSS   *                                            *  * reads and writes are forbidden
  SSS   *                                            *  * tablet stores update is forbidden
  SSS   *                                            *
  SSS   *                                            *  - wait until common dynamic stores are flushed
  SSS   *                                            *
  SSS   *       TReqChangeSmoothMovementStage       (*) ServantSwitchRequested
  SS-  (*) ServantSwitchRequested <----------------- *
  SS-   *  * tablet stores update is forbidden       *
  SS-   *                                            *
  SS-   *  - wait until no tablet store update       *
  SS-   *    is prepared                             *
  SS-   *                                            *
  S--  (*) WaitingForLocksBeforeSwitch               *
  S--   *  * write to tablet is forbidden            *
  S--   *                                            *
  S--   *  - wait until all persistent and           *
  S--   *  - transient transactions finish           *
  S--   *                                            *
  ---  (*) ServantSwitched                           *
  ---   *  * read is forbidden, source is effectively*
  ---   *    outdated from this moment and will be   *
  ---   *    forcefully unmounted soon. All requests *
  ---   *    fail with a certain code so that client *
  ---   *    will resend them to target              *
  ---   *                                            *
  ---   *  - send TReqSwitchServant to master        *
  ---   *  - send TReqSwitchServant to sibling,      *
  ---   *    containing master avenue endpoint cookie*
  ---   *                                            *
  ---   *        TReqSwitchServant                   *
  TTT   * ----------------------------------------> (*) ServantSwitched
  TTT   *                                            *  * start accepting reads, writes and compactions
  TTT   *                                            *  * target is the main servant from this moment
  TTT   *                                            *
  TTT   *                                            *  - send deallocation request to master.
  TTT   *                                            *   TODO: wait for client cache invalidation
  TTT   *                                            *   because client retries rely on source being
  TTT   *                                            *   alive.
  TTT   *                                            *

 */
class TSmoothMovementTracker
    : public TTabletAutomatonPart
    , public ISmoothMovementTracker
{
public:
    TSmoothMovementTracker(
        ISmoothMovementTrackerHostPtr host,
        NHydra::ISimpleHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker)
        : TTabletAutomatonPart(
            host->GetCellId(),
            std::move(hydraManager),
            std::move(automaton),
            std::move(automatonInvoker),
            /*mutationForwarder*/ nullptr)
        , Host_(std::move(host))
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TSmoothMovementTracker::HydraStartSmoothMovement, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TSmoothMovementTracker::HydraAbortSmoothMovement, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TSmoothMovementTracker::HydraChangeSmoothMovementStage, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TSmoothMovementTracker::HydraSwitchServant, Unretained(this)));
    }

    void CheckTablet(TTablet* tablet) override
    {
        if (!IsLeader()) {
            return;
        }

        auto& movementData = tablet->SmoothMovementData();
        if (movementData.GetRole() == ESmoothMovementRole::None) {
            return;
        }

        YT_LOG_DEBUG("Smooth movement tracker observes tablet (%v, Role: %v, Stage: %v)",
            tablet->GetLoggingTag(),
            movementData.GetRole(),
            movementData.GetStage());

        if (movementData.GetStageChangeScheduled()) {
            return;
        }

        std::optional<ESmoothMovementStage> newStage;

        if (ApplyTestingDelayBeforeStageChange(tablet)) {
            return;
        }

        if (movementData.GetRole() == ESmoothMovementRole::Source) {
            switch (movementData.GetStage()) {
                case ESmoothMovementStage::TargetAllocated: {
                    if (tablet->GetStoresUpdatePreparedTransactionId()) {
                        return;
                    }

                    newStage = ESmoothMovementStage::WaitingForLocksBeforeActivation;
                    break;
                }

                case ESmoothMovementStage::WaitingForLocksBeforeActivation: {
                    if (tablet->GetTotalTabletLockCount() > 0) {
                        return;
                    }

                    newStage = ESmoothMovementStage::TargetActivated;
                    break;
                }

                case ESmoothMovementStage::ServantSwitchRequested: {
                    if (tablet->GetStoresUpdatePreparedTransactionId()) {
                        return;
                    }

                    newStage = ESmoothMovementStage::WaitingForLocksBeforeSwitch;
                    break;
                }

                case ESmoothMovementStage::WaitingForLocksBeforeSwitch: {
                    if (tablet->GetTotalTabletLockCount() > 0) {
                        return;
                    }

                    newStage = ESmoothMovementStage::ServantSwitched;
                    break;
                }

                default:
                    break;
            }
        } else if (movementData.GetRole() == ESmoothMovementRole::Target) {
            switch (movementData.GetStage()) {
                case ESmoothMovementStage::TargetActivated:
                    if (!movementData.CommonDynamicStoreIds().empty()) {
                        return;
                    }

                    if (TInstant::Now() - movementData.GetLastStageChangeTime() <
                        GetDynamicConfig()->PreloadWaitTimeout)
                    {
                        // NB: It may be rather slow to traverse all chunks upon each CheckTablet
                        // call since this method is called on all transaction commits.
                        // Maybe should optimize and store accumulated preload statistics in tablet.
                        auto preloadStatistics = tablet->ComputePreloadStatistics();
                        if (preloadStatistics.PendingStoreCount > 0 ||
                            preloadStatistics.FailedStoreCount > 0)
                        {
                            YT_LOG_DEBUG("Target servant is not fully preloaded, will not initiate switch "
                                "(%v, PendingStoreCount: %v, FailedStoreCount: %v)",
                                tablet->GetLoggingTag(),
                                preloadStatistics.PendingStoreCount,
                                preloadStatistics.FailedStoreCount);

                            return;
                        }
                    }

                    newStage = ESmoothMovementStage::ServantSwitchRequested;

                    break;

                case ESmoothMovementStage::ServantSwitched:
                    // TODO(ifsmirnov): wait for cache invalidation.
                    newStage = ESmoothMovementStage::SourceDeactivationRequested;
                    break;

                default:
                    break;
            }
        }

        if (newStage) {
            YT_LOG_DEBUG("Scheduling smooth movement stage change "
                "(%v, Role: %v, OldStage: % v, NewStage: %v)",
                tablet->GetLoggingTag(),
                movementData.GetRole(),
                movementData.GetStage(),
                *newStage);

            movementData.SetStageChangeScheduled(true);

            TReqChangeSmoothMovementStage req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            req.set_mount_revision(ToProto(tablet->GetMountRevision()));
            req.set_expected_stage(ToProto(movementData.GetStage()));
            req.set_new_stage(ToProto(*newStage));

            auto mutation = CreateMutation(HydraManager_, req);
            YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger)
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                    if (!error.IsOK()) {
                        YT_LOG_WARNING(error, "Failed to commit smooth movement stage change mutation");
                    }
                })));
        }
    }

    void OnGotReplicatedContent(TTablet* tablet) override
    {
        ChangeSmoothMovementStage(
            tablet,
            ESmoothMovementStage::TargetAllocated,
            ESmoothMovementStage::TargetActivated);
    }

private:
    const ISmoothMovementTrackerHostPtr Host_;

    void HydraStartSmoothMovement(TReqStartSmoothMovement* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto sourceMountRevision = FromProto<NHydra::TRevision>(request->source_mount_revision());
        auto targetMountRevision = FromProto<NHydra::TRevision>(request->target_mount_revision());

        if (sourceMountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto targetCellId = FromProto<TTabletCellId>(request->target_cell_id());

        auto& movementData = tablet->SmoothMovementData();
        movementData.SetRole(ESmoothMovementRole::Source);
        movementData.SetSiblingCellId(targetCellId);
        movementData.SetStage(ESmoothMovementStage::TargetAllocated);
        movementData.SetLastStageChangeTime(GetCurrentMutationContext()->GetTimestamp());
        movementData.SetSiblingMountRevision(targetMountRevision);

        auto& runtimeData = tablet->RuntimeData()->SmoothMovementData;
        runtimeData.Role = ESmoothMovementRole::Source;
        YT_VERIFY(runtimeData.IsActiveServant.load());
        runtimeData.SiblingServantCellId.Store(targetCellId);
        runtimeData.SiblingServantMountRevision.store(targetMountRevision);

        auto selfEndpointId = FromProto<TAvenueEndpointId>(request->source_avenue_endpoint_id());
        auto siblingEndpointId = GetSiblingAvenueEndpointId(selfEndpointId);
        movementData.SetSiblingAvenueEndpointId(siblingEndpointId);
        Host_->RegisterSiblingTabletAvenue(siblingEndpointId, targetCellId);

        if (request->has_dynamic_store_id()) {
            auto reason = EDynamicStoreIdReservationReason::SmoothMovement;
            tablet->PushDynamicStoreIdToPool(
                FromProto<TStoreId>(request->dynamic_store_id()),
                reason);
            YT_VERIFY(tablet->ReservedDynamicStoreIdCount()[reason] == 1);
        }

        YT_LOG_DEBUG("Smooth tablet movement started (%v, TargetCellId: %v)",
            tablet->GetLoggingTag(),
            targetCellId);

        CheckTablet(tablet);
    }

    void HydraAbortSmoothMovement(TReqAbortSmoothMovement* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsActiveServant());

        const auto& movementData = tablet->SmoothMovementData();

        if (movementData.GetRole() == ESmoothMovementRole::None) {
            // This is fine because abort request may come when the movement had already finished.
            YT_LOG_DEBUG("Attempted to abort smooth movement when it is not in progress (%v)",
                tablet->GetLoggingTag());
            return;
        }

        DoAbortSmoothMovement(tablet, {});

        YT_LOG_DEBUG("Smooth tablet movement aborted by master request (%v)", tablet->GetLoggingTag());
    }

    void HydraChangeSmoothMovementStage(NProto::TReqChangeSmoothMovementStage* request)
    {
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = FromProto<TRevision>(request->mount_revision());
        auto expectedStage = FromProto<ESmoothMovementStage>(request->expected_stage());
        auto newStage = FromProto<ESmoothMovementStage>(request->new_stage());

        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (IsHiveMutation()) {
            auto senderId = GetHiveMutationSenderId();
            YT_VERIFY(IsAvenueEndpointType(TypeFromId(senderId)));

            auto expectedSenderId = tablet->SmoothMovementData().GetSiblingAvenueEndpointId();

            if (senderId != expectedSenderId) {
                YT_LOG_ALERT("Got smooth movement stage change request from invalid sender, ignored "
                    "(%v, ExpectedSenderId: %v, ActualSenderId: %v)",
                    tablet->GetLoggingTag(),
                    expectedSenderId,
                    senderId);
                return;
            }
        }

        if (mountRevision != tablet->GetMountRevision()) {
            YT_LOG_ALERT("Invalid mount revision on smooth movement stage change request, ignored "
                "(%v, ExpectedMountRevision: %x, ActualMountRevision: %x, ExpectedStage: %v, NewStage: %v)",
                tablet->GetLoggingTag(),
                mountRevision,
                tablet->GetMountRevision(),
                expectedStage,
                newStage);
            return;
        }

        ChangeSmoothMovementStage(tablet, expectedStage, newStage);
    }

    void HydraSwitchServant(NProto::TReqSwitchServant* request)
    {
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = FromProto<TRevision>(request->mount_revision());

        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        // NB: This is probably an overkill check since this mutation is only send as an avenue
        // message to the target servant and the only one communicating with it is the source servant.
        if (mountRevision != tablet->GetMountRevision()) {
            YT_LOG_ALERT("Invalid mount revision on servant switch request, ignored "
                "(%v, ExpectedMountRevision: %v, ActualMountRevision: %v)",
                tablet->GetLoggingTag(),
                tablet->GetMountRevision(),
                mountRevision);
            return;
        }

        YT_VERIFY(tablet->SmoothMovementData().GetRole() == ESmoothMovementRole::Target);

        auto masterEndpointId = FromProto<TAvenueEndpointId>(request->master_avenue_endpoint_id());
        auto mailboxCookie = FromProto<TPersistentMailboxStateCookie>(request->master_mailbox_cookie());

        YT_LOG_DEBUG("Got servant switch request (%v, MasterAvenueEndpointId: %v, "
            "FirstOutcomingMessageId: %v)",
            tablet->GetLoggingTag(),
            masterEndpointId,
            mailboxCookie.FirstOutcomingMessageId);

        tablet->SetMasterAvenueEndpointId(masterEndpointId);
        Host_->RegisterMasterAvenue(tabletId, masterEndpointId, std::move(mailboxCookie));
        YT_VERIFY(!tablet->RuntimeData()->SmoothMovementData.IsActiveServant.load());
        tablet->RuntimeData()->SmoothMovementData.IsActiveServant.store(true);

        ChangeSmoothMovementStage(
            tablet,
            ESmoothMovementStage::ServantSwitchRequested,
            ESmoothMovementStage::ServantSwitched);

        if (auto& promise = tablet->SmoothMovementData().TargetActivationPromise()) {
            YT_LOG_DEBUG("Setting target servant activation promise (%v)",
                tablet->GetLoggingTag());
            promise.TrySet();
        }

        tablet->UpdateUnflushedTimestamp();
    }

    void ChangeSmoothMovementStage(
        TTablet* tablet,
        ESmoothMovementStage expectedStage,
        ESmoothMovementStage newStage)
    {
        YT_VERIFY(HasHydraContext());

        auto& movementData = tablet->SmoothMovementData();

        auto tags = Format("%v, Role: %v, Change: %v -> %v, CurrentStage: %v",
            tablet->GetLoggingTag(),
            movementData.GetRole(),
            expectedStage,
            newStage,
            movementData.GetStage());

        if (movementData.GetStage() != expectedStage) {
            YT_LOG_DEBUG("Expected stage mismatch on smooth movement stage change "
                "request, ignored (%v)",
                tags);
            return;
        }

        if (movementData.GetRole() == ESmoothMovementRole::None) {
            YT_LOG_DEBUG("Smooth movement stage change request received by a tablet "
                "not participating in smooth movement, ignored (%v)",
                tags);
            return;
        }

        YT_LOG_DEBUG("Changing smooth movement stage (%v)", tags);

        movementData.SetStage(newStage);
        movementData.SetStageChangeScheduled(false);
        movementData.SetLastStageChangeTime(GetCurrentMutationContext()->GetTimestamp());

        auto role = movementData.GetRole();

        if (role == ESmoothMovementRole::Source) {
            ChangeStageAtSource(tablet, expectedStage, newStage);
        } else if (role == ESmoothMovementRole::Target) {
            ChangeStageAtTarget(tablet, expectedStage, newStage);
        } else {
            YT_ABORT();
        }

        Host_->UpdateTabletSnapshot(tablet);

        NTabletServer::NProto::TReqReportSmoothMovementProgress req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(ToProto(tablet->GetMountRevision()));
        req.set_stage(ToProto(newStage));
        Host_->PostMasterMessage(tablet, req, /*forceCellMailbox*/ true);

        CheckTablet(tablet);
    }

    void ChangeStageAtSource(
        TTablet* tablet,
        ESmoothMovementStage expectedStage,
        ESmoothMovementStage newStage)
    {
        YT_VERIFY(HasHydraContext());

        auto& movementData = tablet->SmoothMovementData();

        if (newStage == ESmoothMovementStage::WaitingForLocksBeforeActivation ||
            newStage == ESmoothMovementStage::WaitingForLocksBeforeSwitch)
        {
            YT_LOG_DEBUG("Tablet approaches smooth movement barrier, aborting all "
                "affecting transactions (%v, Stage: %v)",
                tablet->GetLoggingTag(),
                newStage);
            Host_->AbortAllTransactions(tablet);
        }

        switch (newStage) {
            case ESmoothMovementStage::WaitingForLocksBeforeActivation:
                YT_VERIFY(expectedStage == ESmoothMovementStage::TargetAllocated);
                break;

            case ESmoothMovementStage::TargetActivated: {
                YT_VERIFY(expectedStage == ESmoothMovementStage::WaitingForLocksBeforeActivation);
                YT_VERIFY(tablet->GetTotalTabletLockCount() == 0);
                const auto& tabletWriteManager = tablet->GetTabletWriteManager();
                YT_VERIFY(!tabletWriteManager->HasUnfinishedPersistentTransactions());
                YT_VERIFY(!tabletWriteManager->HasUnfinishedTransientTransactions());

                ReleaseReservedDynamicStore(tablet);

                // TODO(ifsmirnov): YT-17388 - frozen tablets.
                if (tablet->GetActiveStore()) {
                    tablet->GetStoreManager()->Rotate(
                        /*createNewStore*/ true,
                        NLsm::EStoreRotationReason::None,
                        /*allowEmptyStore*/ true);
                }

                SendReplicateTabletContentRequest(tablet);

                break;
            }

            case ESmoothMovementStage::ServantSwitchRequested:
                YT_VERIFY(expectedStage == ESmoothMovementStage::TargetActivated);
                break;

            case ESmoothMovementStage::WaitingForLocksBeforeSwitch:
                YT_VERIFY(expectedStage == ESmoothMovementStage::ServantSwitchRequested);
                break;

            case ESmoothMovementStage::ServantSwitched: {
                YT_VERIFY(expectedStage == ESmoothMovementStage::WaitingForLocksBeforeSwitch);

                YT_LOG_DEBUG("Posting servant switch message (%v, CellId: %v)",
                    tablet->GetLoggingTag(),
                    Host_->GetCellId());

                // Send message to master.
                {
                    NTabletServer::NProto::TReqSwitchServant req;
                    ToProto(req.mutable_tablet_id(), tablet->GetId());
                    req.set_source_mount_revision(ToProto(tablet->GetMountRevision()));
                    req.set_target_mount_revision(ToProto(movementData.GetSiblingMountRevision()));
                    Host_->PostMasterMessage(tablet, req);
                }

                // Send message to sibling servant.
                {
                    TReqSwitchServant req;
                    ToProto(req.mutable_tablet_id(), tablet->GetId());
                    req.set_mount_revision(ToProto(movementData.GetSiblingMountRevision()));

                    auto masterEndpointId = tablet->GetMasterAvenueEndpointId();
                    tablet->SetMasterAvenueEndpointId({});
                    tablet->RuntimeData()->SmoothMovementData.IsActiveServant.store(false);

                    ToProto(req.mutable_master_avenue_endpoint_id(), masterEndpointId);
                    auto mailboxCookie = Host_->UnregisterMasterAvenue(masterEndpointId);
                    ToProto(req.mutable_master_mailbox_cookie(), mailboxCookie);

                    Host_->PostAvenueMessage(
                        movementData.GetSiblingAvenueEndpointId(),
                        req);
                }

                // TODO(ifsmirnov): maybe should unregister sibling avenue here.

                break;
            }

            default:
                YT_ABORT();
        }
    }

    void ChangeStageAtTarget(
        TTablet* tablet,
        ESmoothMovementStage expectedStage,
        ESmoothMovementStage newStage)
    {
        YT_VERIFY(HasHydraContext());

        auto& movementData = tablet->SmoothMovementData();

        switch (newStage) {
            case ESmoothMovementStage::TargetActivated:
                YT_VERIFY(expectedStage == ESmoothMovementStage::TargetAllocated);
                break;

            case ESmoothMovementStage::ServantSwitchRequested: {
                YT_VERIFY(expectedStage == ESmoothMovementStage::TargetActivated);

                TReqChangeSmoothMovementStage req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_mount_revision(ToProto(movementData.GetSiblingMountRevision()));
                req.set_expected_stage(ToProto(ESmoothMovementStage::TargetActivated));
                req.set_new_stage(ToProto(ESmoothMovementStage::ServantSwitchRequested));

                Host_->PostAvenueMessage(
                    movementData.GetSiblingAvenueEndpointId(),
                    req);
                break;
            }

            case ESmoothMovementStage::ServantSwitched:
                YT_VERIFY(expectedStage == ESmoothMovementStage::ServantSwitchRequested);
                break;

            case ESmoothMovementStage::SourceDeactivationRequested: {
                YT_VERIFY(expectedStage == ESmoothMovementStage::ServantSwitched);

                NTabletServer::NProto::TReqDeallocateServant req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_auxiliary_mount_revision(ToProto(movementData.GetSiblingMountRevision()));
                Host_->PostMasterMessage(tablet, req);

                Host_->UnregisterSiblingTabletAvenue(
                    movementData.GetSiblingAvenueEndpointId());

                movementData = {};
                tablet->RuntimeData()->SmoothMovementData.Reset();

                break;
            }

            default:
                YT_ABORT();
        }
    }

    void RejectMovement(TTablet* tablet, const TError& error)
    {
        if (tablet->SmoothMovementData().GetRole() == ESmoothMovementRole::None) {
            return;
        }

        if (!tablet->IsActiveServant()) {
            // At this point tablet has already been successfully moved.
            return;
        }

        DoAbortSmoothMovement(tablet, error);

        YT_LOG_DEBUG(error, "Smooth movement rejected (%v)",
            tablet->GetLoggingTag());
    }

    void SendReplicateTabletContentRequest(TTablet* tablet)
    {
        try {
            auto request = Host_->PrepareReplicateTabletContentRequest(tablet);

            const auto& movementData = tablet->SmoothMovementData();

            YT_LOG_DEBUG("Sending replicate tablet content request "
                "(%v, StoreCount: %v, UnflushedDynamicStoreIds: %v)",
                tablet->GetLoggingTag(),
                request.replicatable_content().stores().size(),
                movementData.CommonDynamicStoreIds());

            Host_->PostAvenueMessage(movementData.GetSiblingAvenueEndpointId(), request);
        } catch (const std::exception& e) {
            RejectMovement(tablet, e);
        }
    }

    void DoAbortSmoothMovement(TTablet* tablet, const TError& error)
    {
        auto& movementData = tablet->SmoothMovementData();

        Host_->UnregisterSiblingTabletAvenue(
            movementData.GetSiblingAvenueEndpointId());

        movementData = {};
        tablet->RuntimeData()->SmoothMovementData.Reset();

        NTabletServer::NProto::TReqReportSmoothMovementAborted rsp;
        ToProto(rsp.mutable_tablet_id(), tablet->GetId());
        ToProto(rsp.mutable_error(), error);
        Host_->PostMasterMessage(tablet, rsp);
    }

    void ReleaseReservedDynamicStore(TTablet* tablet)
    {
        auto reason = EDynamicStoreIdReservationReason::SmoothMovement;

        int reservedCount = tablet->ReservedDynamicStoreIdCount()[reason];
        YT_VERIFY(reservedCount <= 1);

        if (reservedCount == 1) {
            tablet->ReleaseReservedDynamicStoreId(reason);
        }
    }

    bool ApplyTestingDelayBeforeStageChange(TTablet* tablet)
    {
        const auto& movementData = tablet->SmoothMovementData();

        auto testingConfig = GetDynamicConfig()->Testing;
        const auto& map = movementData.GetRole() == ESmoothMovementRole::Source
            ? testingConfig->DelayAfterStageAtSource
            : testingConfig->DelayAfterStageAtTarget;

        auto it = map.find(movementData.GetStage());
        if (it == map.end()) {
            return false;
        }

        auto delay = it->second;
        auto allowedTime = movementData.GetLastStageChangeTime() + delay;
        auto now = TInstant::Now();

        if (now >= allowedTime) {
            return false;
        }

        YT_LOG_DEBUG("Smooth movement stage change delayed for testing purposes "
            "(%v, Role: %v, Stage: %v, RemainingTime: %v)",
            tablet->GetLoggingTag(),
            movementData.GetRole(),
            movementData.GetStage(),
            allowedTime - now);

        TDelayedExecutor::Submit(
            BIND([this, this_ = MakeStrong(this), tabletId = tablet->GetId()] {
                if (auto* tablet = Host_->FindTablet(tabletId)) {
                    CheckTablet(tablet);
                }
            }),
            allowedTime - now,
            AutomatonInvoker_);

        return true;
    }

    TSmoothMovementTrackerDynamicConfigPtr GetDynamicConfig() const
    {
        return Host_->GetDynamicConfig()->SmoothMovementTracker;
    }
};

////////////////////////////////////////////////////////////////////////////////

ISmoothMovementTrackerPtr CreateSmoothMovementTracker(
    ISmoothMovementTrackerHostPtr host,
    NHydra::ISimpleHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
{
    return New<TSmoothMovementTracker>(
        std::move(host),
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
