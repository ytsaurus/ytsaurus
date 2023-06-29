#include "transaction_supervisor.h"
#include "commit.h"
#include "abort.h"
#include "config.h"
#include "transaction_manager.h"
#include "transaction_participant_provider.h"
#include "private.h"

#include <yt/yt/server/lib/transaction_supervisor/proto/transaction_supervisor.pb.h>

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>
#include <yt/yt/server/lib/hydra_common/entity_map.h>
#include <yt/yt/server/lib/hydra_common/hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_service.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>
#include <yt/yt/ytlib/transaction_supervisor/transaction_supervisor_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/hive/transaction_participant.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/api/connection.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NTransactionSupervisor {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc::NProto;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto ParticipantCleanupPeriod = TDuration::Minutes(5);
static const auto ParticipantTtl = TDuration::Minutes(5);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTransactionSupervisor)

class TTransactionSupervisor
    : public TCompositeAutomatonPart
    , public ITransactionSupervisor
{
public:
    TTransactionSupervisor(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        IInvokerPtr trackerInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IResponseKeeperPtr responseKeeper,
        ITransactionManagerPtr transactionManager,
        TCellId selfCellId,
        TClusterTag selfClockClusterTag,
        ITimestampProviderPtr timestampProvider,
        std::vector<ITransactionParticipantProviderPtr> participantProviders,
        IAuthenticatorPtr authenticator)
        : TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , Config_(std::move(config))
        , TrackerInvoker_(std::move(trackerInvoker))
        , HydraManager_(std::move(hydraManager))
        , ResponseKeeper_(std::move(responseKeeper))
        , TransactionManager_(std::move(transactionManager))
        , SelfCellId_(selfCellId)
        , SelfClockClusterTag_(selfClockClusterTag)
        , TimestampProvider_(std::move(timestampProvider))
        , ParticipantProviders_(std::move(participantProviders))
        , Logger(TransactionSupervisorLogger.WithTag("CellId: %v", SelfCellId_))
        , Authenticator_(std::move(authenticator))
        , TransactionSupervisorService_(New<TTransactionSupervisorService>(this))
        , TransactionParticipantService_(New<TTransactionParticipantService>(this))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(TrackerInvoker_);
        YT_VERIFY(ResponseKeeper_);
        YT_VERIFY(TransactionManager_);
        YT_VERIFY(TimestampProvider_);

        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorCommitSimpleTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorCommitSimpleTransaction"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorCommitDistributedTransactionPhaseOne, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorCommitDistributedTransactionPhaseOne"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorCommitDistributedTransactionPhaseTwo, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorCommitDistributedTransactionPhaseTwo"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorAbortDistributedTransactionPhaseTwo, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorAbortDistributedTransactionPhaseTwo"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorAbortTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorAbortTransaction"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraCoordinatorFinishDistributedTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqCoordinatorFinishDistributedTransaction"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraParticipantPrepareTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqParticipantPrepareTransaction"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraParticipantCommitTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqParticipantCommitTransaction"});
        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TTransactionSupervisor::HydraParticipantAbortTransaction, Unretained(this)),
            /*aliases*/ {"NYT.NHiveServer.NProto.TReqParticipantAbortTransaction"});

        RegisterLoader(
            "TransactionSupervisor.Keys",
            BIND(&TTransactionSupervisor::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionSupervisor.Values",
            BIND(&TTransactionSupervisor::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionSupervisor.Keys",
            BIND(&TTransactionSupervisor::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionSupervisor.Values",
            BIND(&TTransactionSupervisor::SaveValues, Unretained(this)));
    }

    std::vector<IServicePtr> GetRpcServices() override
    {
        return std::vector<IServicePtr>{
            TransactionSupervisorService_,
            TransactionParticipantService_
        };
    }

    TFuture<void> CommitTransaction(TTransactionId transactionId) override
    {
        return MessageToError(
            CoordinatorCommitTransaction(
                transactionId,
                {},
                {},
                {},
                false,
                true,
                false,
                ETransactionCoordinatorPrepareMode::Early,
                ETransactionCoordinatorCommitMode::Eager,
                /*maxAllowedCommitTimestamp*/ NullTimestamp,
                NullMutationId,
                GetCurrentAuthenticationIdentity(),
                /*prerequisiteTransactionIds*/ {}));
    }

    TFuture<void> AbortTransaction(
        TTransactionId transactionId,
        bool force) override
    {
        return MessageToError(
            CoordinatorAbortTransaction(
                transactionId,
                NullMutationId,
                force));
    }

    void SetDecommission(bool decommission) override
    {
        YT_VERIFY(HasHydraContext());

        if (decommission == Decommissioned_) {
            return;
        }

        if (decommission) {
            YT_LOG_INFO("Decommissioning transaction supervisor");
        } else {
            YT_LOG_INFO("Transaction supervisor is no longer decommissioned");
        }

        Decommissioned_ = decommission;
    }

    bool IsDecommissioned() const override
    {
        return Decommissioned_ && PersistentCommitMap_.empty();
    }

private:
    const TTransactionSupervisorConfigPtr Config_;
    const IInvokerPtr TrackerInvoker_;
    const IHydraManagerPtr HydraManager_;
    const IResponseKeeperPtr ResponseKeeper_;
    const ITransactionManagerPtr TransactionManager_;
    const TCellId SelfCellId_;
    const TClusterTag SelfClockClusterTag_;
    const ITimestampProviderPtr TimestampProvider_;
    const std::vector<ITransactionParticipantProviderPtr> ParticipantProviders_;

    const NLogging::TLogger Logger;

    const IAuthenticatorPtr Authenticator_;

    TEntityMap<TCommit> TransientCommitMap_;
    TEntityMap<TCommit> PersistentCommitMap_;

    THashMap<TTransactionId, TAbort> TransientAbortMap_;

    bool Decommissioned_ = false;


    class TWrappedParticipant
        : public TRefCounted
    {
    public:
        TWrappedParticipant(
            TCellId cellId,
            TTransactionSupervisorConfigPtr config,
            ITimestampProviderPtr coordinatorTimestampProvider,
            TClusterTag coordinatorClockClusterTag,
            const std::vector<ITransactionParticipantProviderPtr>& providers,
            const NLogging::TLogger logger)
            : CellId_(cellId)
            , Config_(std::move(config))
            , CoordinatorTimestampProvider_(std::move(coordinatorTimestampProvider))
            , CoordinatorClockClusterTag_(coordinatorClockClusterTag)
            , Providers_(providers)
            , ProbationExecutor_(New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetLightInvoker(),
                BIND(&TWrappedParticipant::OnProbation, MakeWeak(this)),
                Config_->ParticipantProbationPeriod))
            , Logger(logger.WithTag("ParticipantCellId: %v", CellId_))
        {
            ProbationExecutor_->Start();
        }

        TCellId GetCellId() const
        {
            return CellId_;
        }

        ETransactionParticipantState GetState()
        {
            auto guard = Guard(SpinLock_);
            auto underlying = GetUnderlying();
            if (!underlying) {
                return ETransactionParticipantState::NotRegistered;
            }
            return underlying->GetState();
        }

        void Touch()
        {
            LastTouched_ = NProfiling::GetInstant();
        }

        bool IsExpired()
        {
            if (GetState() == ETransactionParticipantState::Unregistered) {
                return true;
            }
            if (LastTouched_ + ParticipantTtl < NProfiling::GetInstant() && PendingSenders_.empty()) {
                return true;
            }
            return false;
        }

        bool IsUp()
        {
            auto guard = Guard(SpinLock_);
            return Up_;
        }

        ITimestampProviderPtr GetTimestampProviderOrThrow()
        {
            auto guard = Guard(SpinLock_);

            auto underlying = GetUnderlying();
            if (!underlying) {
                THROW_ERROR MakeUnavailableError();
            }

            return underlying->GetTimestampProvider();
        }

        TFuture<void> PrepareTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                false,
                true,
                commit,
                [
                    this,
                    this_ = MakeStrong(this),
                    transactionId = commit->GetTransactionId(),
                    generatePrepareTimestamp = commit->GetGeneratePrepareTimestamp(),
                    inheritCommitTimestamp = commit->GetInheritCommitTimestamp(),
                    cellIdsToSyncWith = commit->CellIdsToSyncWithBeforePrepare(),
                    identity = commit->AuthenticationIdentity()
                ]
                (const ITransactionParticipantPtr& participant) {
                    auto prepareTimestamp = GeneratePrepareTimestamp(
                        participant,
                        generatePrepareTimestamp,
                        inheritCommitTimestamp);
                    return participant->PrepareTransaction(
                        transactionId,
                        prepareTimestamp,
                        GetTimestampClusterTag(participant, inheritCommitTimestamp),
                        cellIdsToSyncWith,
                        identity);
                });
        }

        TFuture<void> CommitTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                true,
                false,
                commit,
                [
                    this,
                    this_ = MakeStrong(this),
                    transactionId = commit->GetTransactionId(),
                    inheritCommitTimestamp = commit->GetInheritCommitTimestamp(),
                    commitTimestamps = commit->CommitTimestamps(),
                    identity = commit->AuthenticationIdentity()
                ]
                (const ITransactionParticipantPtr& participant) {
                    auto cellTag = CellTagFromId(participant->GetCellId());
                    auto commitTimestamp = commitTimestamps.GetTimestamp(cellTag);
                    return participant->CommitTransaction(
                        transactionId,
                        commitTimestamp,
                        GetTimestampClusterTag(participant, inheritCommitTimestamp),
                        identity);
                });
        }

        TFuture<void> AbortTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                true,
                false,
                commit,
                [
                    transactionId = commit->GetTransactionId(),
                    identity = commit->AuthenticationIdentity()
                ]
                (const ITransactionParticipantPtr& participant) {
                    return participant->AbortTransaction(
                        transactionId,
                        identity);
                });
        }

        void SetUp()
        {
            auto guard = Guard(SpinLock_);

            if (Up_) {
                return;
            }

            decltype(PendingSenders_) senders;
            PendingSenders_.swap(senders);
            Up_ = true;

            guard.Release();

            YT_LOG_DEBUG("Participant cell is up");

            for (const auto& sender : senders) {
                sender();
            }
        }

        void SetDown(const TError& error)
        {
            auto guard = Guard(SpinLock_);

            if (!Up_) {
                return;
            }

            Up_ = false;

            YT_LOG_DEBUG(error, "Participant cell is down");
        }

    private:
        const TCellId CellId_;
        const TTransactionSupervisorConfigPtr Config_;
        const ITimestampProviderPtr CoordinatorTimestampProvider_;
        const TClusterTag CoordinatorClockClusterTag_;
        const std::vector<ITransactionParticipantProviderPtr> Providers_;
        const TPeriodicExecutorPtr ProbationExecutor_;
        const NLogging::TLogger Logger;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
        ITransactionParticipantPtr Underlying_;
        std::vector<TClosure> PendingSenders_;
        bool Up_ = true;

        TInstant LastTouched_;


        ITransactionParticipantPtr GetUnderlying()
        {
            if (!Underlying_) {
                Underlying_ = TryCreateUnderlying();
            }
            return Underlying_;
        }

        ITransactionParticipantPtr TryCreateUnderlying()
        {
            TTransactionParticipantOptions options{
                .RpcTimeout = Config_->RpcTimeout
            };

            for (const auto& provider : Providers_) {
                if (auto participant = provider->TryCreate(CellId_, options)) {
                    return participant;
                }
            }

            YT_LOG_DEBUG("Could not find any matching transaction participant provider");

            return nullptr;
        }


        template <class F>
        TFuture<void> EnqueueRequest(
            bool succeedOnUnregistered,
            bool mustSendImmediately,
            TCommit* commit,
            F func)
        {
            auto promise = NewPromise<void>();

            auto guard = Guard(SpinLock_);

            auto underlying = GetUnderlying();

            if (!underlying) {
                return MakeFuture<void>(MakeUnavailableError());
            }

            // Fast path.
            if (Up_ && underlying->GetState() == ETransactionParticipantState::Valid) {
                // Make a copy, commit may die.
                auto identity = commit->AuthenticationIdentity();
                NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

                return func(underlying);
            }

            // Slow path.
            auto sender = [=, this, this_ = MakeStrong(this), underlying = std::move(underlying), identity = commit->AuthenticationIdentity()] {
                NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);
                switch (underlying->GetState()) {
                    case ETransactionParticipantState::Valid:
                        promise.SetFrom(func(underlying));
                        break;

                    case ETransactionParticipantState::Unregistered:
                        if (succeedOnUnregistered) {
                            YT_LOG_DEBUG("Participant unregistered; assuming success");
                            promise.Set(TError());
                        } else {
                            promise.Set(MakeUnregisteredError());
                        }
                        break;

                    case ETransactionParticipantState::Invalidated:
                        promise.Set(MakeInvalidatedError());
                        break;

                    default:
                        YT_ABORT();
                }
            };

            if (Up_) {
                guard.Release();
                sender();
            } else {
                if (mustSendImmediately) {
                    return MakeFuture<void>(MakeDownError());
                }
                PendingSenders_.push_back(BIND(std::move(sender)));
            }

            return promise;
        }


        void OnProbation()
        {
            auto guard = Guard(SpinLock_);

            if (Up_) {
                return;
            }

            if (PendingSenders_.empty()) {
                guard.Release();
                CheckAvailability();
            } else {
                auto sender = std::move(PendingSenders_.back());
                PendingSenders_.pop_back();

                guard.Release();

                sender();
            }
        }

        void CheckAvailability()
        {
            auto guard = Guard(SpinLock_);

            auto underlying = GetUnderlying();
            if (!underlying) {
                return;
            }

            guard.Release();

            auto state = underlying->GetState();
            if (state != ETransactionParticipantState::Valid) {
                return;
            }

            YT_LOG_DEBUG("Checking participant availablitity");
            underlying->CheckAvailability().Subscribe(
                BIND(&TWrappedParticipant::OnAvailabilityCheckResult, MakeWeak(this)));
        }

        void OnAvailabilityCheckResult(const TError& error)
        {
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Participant availability check failed");
                return;
            }

            SetUp();
        }


        TError MakeUnavailableError() const
        {
            return TError(
                NRpc::EErrorCode::Unavailable,
                "Participant cell %v is currently unavailable",
                CellId_);
        }

        TError MakeDownError() const
        {
            return TError(
                NRpc::EErrorCode::Unavailable,
                "Participant cell %v is currently down",
                CellId_);
        }

        TError MakeUnregisteredError() const
        {
            return TError(
                NHiveClient::EErrorCode::ParticipantUnregistered,
                "Participant cell %v is unregistered",
                CellId_);
        }

        TError MakeInvalidatedError() const
        {
            return TError(
                NRpc::EErrorCode::Unavailable,
                "Participant cell %v was invalidated",
                CellId_);
        }


        TTimestamp GeneratePrepareTimestamp(
            const ITransactionParticipantPtr& participant,
            bool generatePrepareTimestamp,
            bool inheritCommitTimestamp)
        {
            if (!generatePrepareTimestamp) {
                return NullTimestamp;
            }
            const auto& timestampProvider = inheritCommitTimestamp
                ? CoordinatorTimestampProvider_
                : participant->GetTimestampProvider();
            return timestampProvider->GetLatestTimestamp();
        }

        TClusterTag GetTimestampClusterTag(
            const ITransactionParticipantPtr& participant,
            bool inheritCommitTimestamp) const
        {
            if (inheritCommitTimestamp) {
                return CoordinatorClockClusterTag_;
            } else {
                return participant->GetClockClusterTag();
            }
        }
    };

    using TWrappedParticipantPtr = TIntrusivePtr<TWrappedParticipant>;

    THashMap<TCellId, TWrappedParticipantPtr> ParticipantMap_;
    TPeriodicExecutorPtr ParticipantCleanupExecutor_;



    class TOwnedServiceBase
        : public THydraServiceBase
    {
    protected:
        explicit TOwnedServiceBase(
            TTransactionSupervisorPtr owner,
            const TServiceDescriptor& descriptor)
            : THydraServiceBase(
                owner->HydraManager_,
                owner->HydraManager_->CreateGuardedAutomatonInvoker(owner->AutomatonInvoker_),
                descriptor,
                TransactionSupervisorLogger,
                owner->SelfCellId_,
                CreateHydraManagerUpstreamSynchronizer(owner->HydraManager_),
                owner->Authenticator_)
            , Owner_(owner)
        { }

        TTransactionSupervisorPtr GetOwnerOrThrow()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service is shutting down");
            }
            return owner;
        }

    private:
        const TWeakPtr<TTransactionSupervisor> Owner_;
    };


    class TTransactionSupervisorService
        : public TOwnedServiceBase
    {
    public:
        explicit TTransactionSupervisorService(TTransactionSupervisorPtr owner)
            : TOwnedServiceBase(
                owner,
                TTransactionSupervisorServiceProxy::GetDescriptor())
        {
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction)
                .SetHeavy(true));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction)
                .SetHeavy(true));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction)
                .SetInvoker(owner->TrackerInvoker_));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(GetDownedParticipants)
                .SetHeavy(true));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionSupervisor, CommitTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto participantCellIds = FromProto<std::vector<TCellId>>(request->participant_cell_ids());
            auto prepareOnlyParticipantCellIds = FromProto<std::vector<TCellId>>(request->prepare_only_participant_cell_ids());
            auto cellIdsToSyncWithBeforePrepare = FromProto<std::vector<TCellId>>(request->cell_ids_to_sync_with_before_prepare());
            auto force2PC = request->force_2pc();
            auto generatePrepareTimestamp = request->generate_prepare_timestamp();
            auto inheritCommitTimestamp = request->inherit_commit_timestamp();
            auto coordinatorCommitMode = CheckedEnumCast<ETransactionCoordinatorCommitMode>(request->coordinator_commit_mode());
            auto coordinatorPrepareMode = CheckedEnumCast<ETransactionCoordinatorPrepareMode>(request->coordinator_prepare_mode());
            auto clockClusterTag = request->has_clock_cluster_tag()
                ? FromProto<TCellTag>(request->clock_cluster_tag())
                : InvalidCellTag;
            auto maxAllowedCommitTimestamp = request->max_allowed_commit_timestamp();
            std::vector<TTransactionId> prerequisiteTransactionIds;
            if (context->GetRequestHeader().HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
                auto* prerequisitesExt = &context->GetRequestHeader().GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
                for (const auto& prerequisite : prerequisitesExt->transactions()) {
                    prerequisiteTransactionIds.push_back(FromProto<TTransactionId>(prerequisite.transaction_id()));
                }
            }

            if ((force2PC || !participantCellIds.empty()) && !prerequisiteTransactionIds.empty()) {
                THROW_ERROR_EXCEPTION("Prerequisite transactions are not supported for distributed transaction commits")
                    << TErrorAttribute("transaction_id", transactionId)
                    << TErrorAttribute("force_2pc", force2PC)
                    << TErrorAttribute("participant_cell_ids", participantCellIds)
                    << TErrorAttribute("prerequisite_transaction_ids", prerequisiteTransactionIds);
            }

            if (coordinatorPrepareMode == ETransactionCoordinatorPrepareMode::Late &&
                coordinatorCommitMode == ETransactionCoordinatorCommitMode::Lazy)
            {
                THROW_ERROR_EXCEPTION("Coordinator prepare and commit modes are incompatible")
                    << TErrorAttribute("coordinator_prepare_mode", coordinatorPrepareMode)
                    << TErrorAttribute("coordinator_commit_mode", coordinatorCommitMode);
            }

            auto owner = GetOwnerOrThrow();

            if (clockClusterTag != InvalidCellTag &&
                owner->SelfClockClusterTag_ != InvalidCellTag &&
                clockClusterTag != owner->SelfClockClusterTag_)
            {
                THROW_ERROR_EXCEPTION("Transaction origin clock source differs from coordinator clock source")
                    << TErrorAttribute("transaction_id", transactionId)
                    << TErrorAttribute("client_clock_cluster_tag", clockClusterTag)
                    << TErrorAttribute("coordinator_clock_cluster_tag", owner->SelfClockClusterTag_);
            }

            context->SetRequestInfo("TransactionId: %v, ParticipantCellIds: %v, PrepareOnlyParticipantCellIds: %v, CellIdsToSyncWithBeforePrepare: %v, "
                "Force2PC: %v, GeneratePrepareTimestamp: %v, InheritCommitTimestamp: %v, ClockClusterTag: %v, "
                "CoordinatorPrepareMode: %v, CoordinatorCommitMode: %v, PrerequisiteTransactionIds: %v, MaxAllowedCommitTimestamp: %v",
                transactionId,
                participantCellIds,
                prepareOnlyParticipantCellIds,
                cellIdsToSyncWithBeforePrepare,
                force2PC,
                generatePrepareTimestamp,
                inheritCommitTimestamp,
                clockClusterTag,
                coordinatorPrepareMode,
                coordinatorCommitMode,
                prerequisiteTransactionIds,
                maxAllowedCommitTimestamp);

            if (owner->TransactionManager_->CommitTransaction(context)) {
                return;
            }

            // NB: Custom abort handler takes care of response keeper itself.
            if (owner->ResponseKeeper_->TryReplyFrom(context, /*subscribeToResponse*/ false)) {
                return;
            }

            // NB: cellIdsToSyncWithBeforePrepare is only respected by participants, not the coordinator.
            auto readyEvent = owner->TransactionManager_->GetReadyToPrepareTransactionCommit(
                prerequisiteTransactionIds,
                /*cellIdsToSyncWith*/ {});

            TFuture<TSharedRefArray> asyncResponseMessage;
            if (readyEvent.IsSet() && readyEvent.Get().IsOK()) {
                // Most likely path.
                asyncResponseMessage = owner->CoordinatorCommitTransaction(
                    transactionId,
                    participantCellIds,
                    prepareOnlyParticipantCellIds,
                    cellIdsToSyncWithBeforePrepare,
                    force2PC,
                    generatePrepareTimestamp,
                    inheritCommitTimestamp,
                    coordinatorPrepareMode,
                    coordinatorCommitMode,
                    maxAllowedCommitTimestamp,
                    context->GetMutationId(),
                    GetCurrentAuthenticationIdentity(),
                    std::move(prerequisiteTransactionIds));
            } else {
                auto mutationId = context->GetMutationId();
                auto identity = GetCurrentAuthenticationIdentity();
                asyncResponseMessage = readyEvent.Apply(
                    BIND([=, owner = std::move(owner), prerequisiteTransactionIds = std::move(prerequisiteTransactionIds)] () {
                    return owner->CoordinatorCommitTransaction(
                        transactionId,
                        participantCellIds,
                        prepareOnlyParticipantCellIds,
                        cellIdsToSyncWithBeforePrepare,
                        force2PC,
                        generatePrepareTimestamp,
                        inheritCommitTimestamp,
                        coordinatorPrepareMode,
                        coordinatorCommitMode,
                        maxAllowedCommitTimestamp,
                        mutationId,
                        identity,
                        std::move(prerequisiteTransactionIds));
                })
                .AsyncVia(GetCurrentInvoker()));
            }

            context->ReplyFrom(asyncResponseMessage);
        }

        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionSupervisor, AbortTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            bool force = request->force();

            context->SetRequestInfo("TransactionId: %v, Force: %v",
                transactionId,
                force);

            auto owner = GetOwnerOrThrow();

            if (owner->TransactionManager_->AbortTransaction(context)) {
                return;
            }

            // NB: Custom abort handler takes care of response keeper itself.
            if (owner->ResponseKeeper_->TryReplyFrom(context, /*subscribeToResponse*/ false)) {
                return;
            }

            auto asyncResponseMessage = owner->CoordinatorAbortTransaction(
                transactionId,
                context->GetMutationId(),
                force);
            context->ReplyFrom(asyncResponseMessage);
        }

        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionSupervisor, PingTransaction)
        {
            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            bool pingAncestors = request->ping_ancestors();

            context->SetRequestInfo("TransactionId: %v, PingAncestors: %v",
                transactionId,
                pingAncestors);

            auto owner = GetOwnerOrThrow();

            // Any exception thrown here is replied to the client.
            owner->TransactionManager_->PingTransaction(transactionId, pingAncestors);

            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionSupervisor, GetDownedParticipants)
        {
            auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

            context->SetRequestInfo("CellCount: %v",
                cellIds.size());

            auto owner = GetOwnerOrThrow();
            auto downedCellIds = owner->GetDownedParticipants(cellIds);

            auto* responseCellIds = context->Response().mutable_cell_ids();
            ToProto(responseCellIds, downedCellIds);

            context->SetResponseInfo("DownedCellCount: %v",
                downedCellIds.size());

            context->Reply();
        }
    };

    const TIntrusivePtr<TTransactionSupervisorService> TransactionSupervisorService_;


    class TTransactionParticipantService
        : public TOwnedServiceBase
    {
    public:
        explicit TTransactionParticipantService(TTransactionSupervisorPtr owner)
            : TOwnedServiceBase(
                owner,
                TTransactionParticipantServiceProxy::GetDescriptor())
        {
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareTransaction)
                .SetHeavy(true));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction)
                .SetHeavy(true));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction)
                .SetHeavy(true));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionParticipant, PrepareTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto prepareTimestamp = request->prepare_timestamp();
            auto prepareTimestampClusterTag = request->prepare_timestamp_cluster_tag();
            auto cellIdsToSyncWith = FromProto<std::vector<TCellId>>(request->cell_ids_to_sync_with());

            context->SetRequestInfo("TransactionId: %v, PrepareTimestamp: %v@%v, CellIdsToSyncWith: %v",
                transactionId,
                prepareTimestamp,
                prepareTimestampClusterTag,
                cellIdsToSyncWith);

            NTransactionSupervisor::NProto::TReqParticipantPrepareTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_prepare_timestamp(prepareTimestamp);
            hydraRequest.set_prepare_timestamp_cluster_tag(prepareTimestampClusterTag);
            NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, NRpc::GetCurrentAuthenticationIdentity());

            auto owner = GetOwnerOrThrow();

            auto readyEvent = owner->TransactionManager_->GetReadyToPrepareTransactionCommit(
                {} /*prerequisiteTransactionIds*/,
                cellIdsToSyncWith);

            auto mutation = CreateMutation(owner->HydraManager_, hydraRequest);
            mutation->SetCurrentTraceContext();

            auto callback = [mutation = std::move(mutation), context] {
                YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
            };

            if (readyEvent.IsSet() && readyEvent.Get().IsOK()) {
                callback();
            } else {
                readyEvent.Subscribe(BIND([callback = BIND(std::move(callback)), context, invoker = owner->EpochAutomatonInvoker_] (const TError& error) {
                    if (error.IsOK()) {
                        invoker->Invoke(std::move(callback));
                    } else {
                        context->Reply(error);
                    }
                }));
            }
        }

        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionParticipant, CommitTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto commitTimestamp = request->commit_timestamp();
            auto commitTimestampClusterTag = request->commit_timestamp_cluster_tag();

            context->SetRequestInfo("TransactionId: %v, CommitTimestamp: %v@%v",
                transactionId,
                commitTimestamp,
                commitTimestampClusterTag);

            NTransactionSupervisor::NProto::TReqParticipantCommitTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(commitTimestamp);
            hydraRequest.set_commit_timestamp_cluster_tag(commitTimestampClusterTag);
            NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, NRpc::GetCurrentAuthenticationIdentity());

            auto owner = GetOwnerOrThrow();
            auto mutation = CreateMutation(owner->HydraManager_, hydraRequest);
            mutation->SetCurrentTraceContext();
            YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
        }

        DECLARE_RPC_SERVICE_METHOD(NProto::NTransactionParticipant, AbortTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());

            context->SetRequestInfo("TransactionId: %v",
                transactionId);

            NTransactionSupervisor::NProto::TReqParticipantAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, NRpc::GetCurrentAuthenticationIdentity());

            auto owner = GetOwnerOrThrow();
            auto mutation = CreateMutation(owner->HydraManager_, hydraRequest);
            mutation->SetCurrentTraceContext();
            YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
        }
    };

    const TIntrusivePtr<TTransactionParticipantService> TransactionParticipantService_;


    // Coordinator implementation.

    TFuture<TSharedRefArray> CoordinatorCommitTransaction(
        TTransactionId transactionId,
        std::vector<TCellId> participantCellIds,
        std::vector<TCellId> prepareOnlyParticipantCellIds,
        std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
        bool force2PC,
        bool generatePrepareTimestamp,
        bool inheritCommitTimestamp,
        ETransactionCoordinatorPrepareMode coordinatorPrepareMode,
        ETransactionCoordinatorCommitMode coordinatorCommitMode,
        TTimestamp maxAllowedCommitTimestamp,
        TMutationId mutationId,
        const TAuthenticationIdentity& identity,
        std::vector<TTransactionId> prerequisiteTransactionIds)
    {
        YT_VERIFY(!HasMutationContext());

        auto* commit = FindCommit(transactionId);
        if (commit) {
            // NB: Even Response Keeper cannot protect us from this.
            return commit->GetAsyncResponseMessage();
        }

        bool distributed =
            force2PC ||
            !participantCellIds.empty();
        commit = CreateTransientCommit(
            transactionId,
            mutationId,
            std::move(participantCellIds),
            std::move(prepareOnlyParticipantCellIds),
            std::move(cellIdsToSyncWithBeforePrepare),
            distributed,
            generatePrepareTimestamp,
            inheritCommitTimestamp,
            coordinatorPrepareMode,
            coordinatorCommitMode,
            maxAllowedCommitTimestamp,
            identity,
            std::move(prerequisiteTransactionIds));

        // Commit instance may die below.
        auto asyncResponseMessage = commit->GetAsyncResponseMessage();

        if (commit->GetDistributed()) {
            CommitDistributedTransaction(commit);
        } else {
            CommitSimpleTransaction(commit);
        }

        return asyncResponseMessage;
    }

    void CommitSimpleTransaction(TCommit* commit)
    {
        YT_VERIFY(!commit->GetPersistent());

        // Make a copy, commit may die.
        auto identity = commit->AuthenticationIdentity();
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto transactionId = commit->GetTransactionId();

        try {
            // Any exception thrown here is replied to the client.
            auto prepareTimestamp = TimestampProvider_->GetLatestTimestamp();

            TTransactionPrepareOptions options{
                .Persistent = false,
                .PrepareTimestamp = prepareTimestamp,
                .PrepareTimestampClusterTag = SelfClockClusterTag_,
                .PrerequisiteTransactionIds = commit->PrerequisiteTransactionIds()
            };
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error preparing simple transaction commit (TransactionId: %v, %v)",
                transactionId,
                NRpc::GetCurrentAuthenticationIdentity());
            SetCommitFailed(commit, ex);
            RemoveTransientCommit(commit);
            // Best effort, fire-and-forget.
            YT_UNUSED_FUTURE(AbortTransaction(transactionId, true));
            return;
        }

        GenerateCommitTimestamps(commit);
    }

    void CommitDistributedTransaction(TCommit* commit)
    {
        YT_VERIFY(!commit->GetPersistent());

        auto prepareTimestamp = commit->GetGeneratePrepareTimestamp()
            ? TimestampProvider_->GetLatestTimestamp()
            : NullTimestamp;

        NTransactionSupervisor::NProto::TReqCoordinatorCommitDistributedTransactionPhaseOne request;
        ToProto(request.mutable_transaction_id(), commit->GetTransactionId());
        ToProto(request.mutable_mutation_id(), commit->GetMutationId());
        ToProto(request.mutable_participant_cell_ids(), commit->ParticipantCellIds());
        ToProto(request.mutable_prepare_only_participant_cell_ids(), commit->PrepareOnlyParticipantCellIds());
        ToProto(request.mutable_cell_ids_to_sync_with_before_prepare(), commit->CellIdsToSyncWithBeforePrepare());
        request.set_generate_prepare_timestamp(commit->GetGeneratePrepareTimestamp());
        request.set_inherit_commit_timestamp(commit->GetInheritCommitTimestamp());
        request.set_coordinator_commit_mode(ToProto<int>(commit->GetCoordinatorCommitMode()));
        request.set_coordinator_prepare_mode(ToProto<int>(commit->GetCoordinatorPrepareMode()));
        request.set_prepare_timestamp(prepareTimestamp);
        request.set_prepare_timestamp_cluster_tag(ToProto<int>(SelfClockClusterTag_));
        request.set_max_allowed_commit_timestamp(commit->GetMaxAllowedCommitTimestamp());
        WriteAuthenticationIdentityToProto(&request, commit->AuthenticationIdentity());

        auto mutation = CreateMutation(HydraManager_, request);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
    }

    TFuture<TSharedRefArray> CoordinatorAbortTransaction(
        TTransactionId transactionId,
        TMutationId mutationId,
        bool force)
    {
        auto* abort = FindAbort(transactionId);
        if (abort) {
            // NB: Even Response Keeper cannot protect us from this.
            return abort->GetAsyncResponseMessage();
        }

        abort = CreateAbort(transactionId, mutationId);

        // Abort instance may die below.
        auto asyncResponseMessage = abort->GetAsyncResponseMessage();

        try {
            // Any exception thrown here is caught below.
            TTransactionAbortOptions options{
                .Force = force
            };
            TransactionManager_->PrepareTransactionAbort(transactionId, options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error preparing transaction abort (TransactionId: %v, Force: %v, %v)",
                transactionId,
                force,
                NRpc::GetCurrentAuthenticationIdentity());
            SetAbortFailed(abort, ex);
            RemoveAbort(abort);
            return asyncResponseMessage;
        }

        NTransactionSupervisor::NProto::TReqCoordinatorAbortTransaction request;
        ToProto(request.mutable_transaction_id(), transactionId);
        ToProto(request.mutable_mutation_id(), mutationId);
        request.set_force(force);

        auto mutation = CreateMutation(HydraManager_, request);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));

        return asyncResponseMessage;
    }

    std::vector<TCellId> GetDownedParticipants(const std::vector<TCellId>& cellIds)
    {
        std::vector<TCellId> result;

        auto considerParticipant = [&] (const TWrappedParticipantPtr& participant) {
            if (participant->GetCellId() != SelfCellId_ && !participant->IsUp()) {
                result.push_back(participant->GetCellId());
            }
        };

        if (cellIds.empty()) {
            for (const auto& [cellId, participant] : ParticipantMap_) {
                considerParticipant(participant);
            }
        } else {
            for (auto cellId : cellIds) {
                auto it = ParticipantMap_.find(cellId);
                if (it != ParticipantMap_.end()) {
                    considerParticipant(it->second);
                }
            }
        }

        return result;
    }


    static TFuture<void> MessageToError(TFuture<TSharedRefArray> asyncMessage)
    {
        return asyncMessage.Apply(BIND([] (const TSharedRefArray& message) -> TFuture<void> {
            TResponseHeader header;
            YT_VERIFY(TryParseResponseHeader(message, &header));
            return header.has_error()
                ? MakeFuture<void>(FromProto<TError>(header.error()))
                : VoidFuture;
        }));
    }

    // Hydra handlers.

    void HydraCoordinatorCommitSimpleTransaction(NTransactionSupervisor::NProto::TReqCoordinatorCommitSimpleTransaction* request)
    {
        auto mutationId = FromProto<TMutationId>(request->mutation_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamps = FromProto<TTimestampMap>(request->commit_timestamps());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto* commit = FindCommit(transactionId);

        if (commit && commit->GetPersistentState() != ECommitState::Start) {
            YT_LOG_DEBUG("Requested to commit simple transaction in wrong state; ignored "
                "(TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        if (commit) {
            commit->CommitTimestamps() = commitTimestamps;
        }

        try {
            // Any exception thrown here is caught below.
            auto commitTimestamp = commitTimestamps.GetTimestamp(CellTagFromId(SelfCellId_));
            TTransactionCommitOptions options{
                .CommitTimestamp = commitTimestamp,
                .CommitTimestampClusterTag = SelfClockClusterTag_
            };
            TransactionManager_->CommitTransaction(transactionId, options);
        } catch (const std::exception& ex) {
            if (commit) {
                SetCommitFailed(commit, ex);
                RemoveTransientCommit(commit);
            }
            YT_LOG_DEBUG(ex, "Error committing simple transaction (TransactionId: %v)",
                transactionId);

            // COMPAT(gritukan)
            auto* mutationContext = GetCurrentMutationContext();
            if (mutationContext->Request().Reign >= 13 && IsLeader()) {
                // Best effort, fire-and-forget.
                YT_UNUSED_FUTURE(AbortTransaction(transactionId, /*force*/ true));
            }
            return;
        }

        if (!commit) {
            // Commit could be missing (e.g. at followers or during recovery).
            // Let's recreate it since it's needed below in SetCommitSucceeded.
            commit = CreateTransientCommit(
                transactionId,
                mutationId,
                /*participantCellIds*/ {},
                /*prepareOnlyParticipantCellIds*/ {},
                /*cellIdsToSyncWithBeforePrepare*/ {},
                /*distributed*/ false,
                /*generatePrepareTimestamp*/ true,
                /*inheritCommitTimestamp*/ false,
                ETransactionCoordinatorPrepareMode::Early,
                ETransactionCoordinatorCommitMode::Eager,
                /* maxAllowedCommitTimestamp*/ NullTimestamp,
                identity,
                /*prerequisiteTransactionIds*/ {});
            commit->CommitTimestamps() = commitTimestamps;
        }

        SetCommitSucceeded(commit);
        RemoveTransientCommit(commit);

        // Transaction may have been (unsuccessfully) aborted. Cached abort errors should not outlive the commit.
        TryRemoveAbort(transactionId);
    }

    void HydraCoordinatorCommitDistributedTransactionPhaseOne(NTransactionSupervisor::NProto::TReqCoordinatorCommitDistributedTransactionPhaseOne* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mutationId = FromProto<TMutationId>(request->mutation_id());
        auto participantCellIds = FromProto<std::vector<TCellId>>(request->participant_cell_ids());
        auto prepareOnlyParticipantCellIds = FromProto<std::vector<TCellId>>(request->prepare_only_participant_cell_ids());
        auto cellIdsToSyncWithBeforePrepare = FromProto<std::vector<TCellId>>(request->cell_ids_to_sync_with_before_prepare());
        auto generatePrepareTimestamp = request->generate_prepare_timestamp();
        auto inheritCommitTimestamp = request->inherit_commit_timestamp();
        auto coordinatorCommitMode = CheckedEnumCast<ETransactionCoordinatorCommitMode>(request->coordinator_commit_mode());
        auto coordinatorPrepareMode = CheckedEnumCast<ETransactionCoordinatorPrepareMode>(request->coordinator_prepare_mode());
        auto prepareTimestamp = request->prepare_timestamp();
        auto prepareTimestampClusterTag = FromProto<TClusterTag>(request->prepare_timestamp_cluster_tag());
        auto maxAllowedCommitTimestamp = request->max_allowed_commit_timestamp();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        // Ensure commit existence (possibly moving it from transient to persistent).
        TCommit* commit;
        try {
            commit = GetOrCreatePersistentCommit(
                transactionId,
                mutationId,
                participantCellIds,
                prepareOnlyParticipantCellIds,
                cellIdsToSyncWithBeforePrepare,
                true,
                generatePrepareTimestamp,
                inheritCommitTimestamp,
                coordinatorPrepareMode,
                coordinatorCommitMode,
                maxAllowedCommitTimestamp,
                identity);
        } catch (const std::exception& ex) {
            if (auto commit = FindCommit(transactionId)) {
                YT_VERIFY(!commit->GetPersistent());
                SetCommitFailed(commit, ex);
                RemoveTransientCommit(commit);
            }
            throw;
        }

        if (commit && commit->GetPersistentState() != ECommitState::Start) {
            YT_LOG_DEBUG(
                "Requested to commit distributed transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        commit->PrepareTimestamp() = prepareTimestamp;
        commit->PrepareTimestampClusterTag() = prepareTimestampClusterTag;

        YT_LOG_DEBUG(
            "Distributed commit phase one started (TransactionId: %v, %v, ParticipantCellIds: %v, PrepareTimestamp: %v@%v)",
            transactionId,
            NRpc::GetCurrentAuthenticationIdentity(),
            participantCellIds,
            prepareTimestamp,
            prepareTimestampClusterTag);

        if (coordinatorPrepareMode == ETransactionCoordinatorPrepareMode::Early &&
            !RunCoordinatorPrepare(commit))
        {
            return;
        }

        ChangeCommitPersistentState(commit, ECommitState::Prepare);
        ChangeCommitTransientState(commit, ECommitState::Prepare);
    }

    void HydraCoordinatorCommitDistributedTransactionPhaseTwo(NTransactionSupervisor::NProto::TReqCoordinatorCommitDistributedTransactionPhaseTwo* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamps = FromProto<TTimestampMap>(request->commit_timestamps());

        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            YT_LOG_ERROR("Requested to execute phase two commit for a non-existing transaction; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        YT_LOG_DEBUG(
            "Distributed commit phase two started "
            "(TransactionId: %v, ParticipantCellIds: %v, PrepareOnlyParticipantCellIds: %v, CommitTimestamps: %v)",
            transactionId,
            commit->ParticipantCellIds(),
            commit->PrepareOnlyParticipantCellIds(),
            commitTimestamps);

        YT_VERIFY(commit->GetDistributed());
        YT_VERIFY(commit->GetPersistent());

        if (commit->GetPersistentState() != ECommitState::Prepare) {
            YT_LOG_ERROR(
                "Requested to execute phase two commit for transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        if (commit->GetCoordinatorPrepareMode() == ETransactionCoordinatorPrepareMode::Late &&
            !RunCoordinatorPrepare(commit))
        {
            return;
        }

        commit->CommitTimestamps() = commitTimestamps;
        ChangeCommitPersistentState(commit, ECommitState::Commit);
        ChangeCommitTransientState(commit, ECommitState::Commit);

        if (commit->GetCoordinatorCommitMode() == ETransactionCoordinatorCommitMode::Eager ||
            commit->GetCoordinatorPrepareMode() == ETransactionCoordinatorPrepareMode::Late)
        {
            RunCoordinatorCommit(commit);
        }
    }

    void HydraCoordinatorAbortDistributedTransactionPhaseTwo(NTransactionSupervisor::NProto::TReqCoordinatorAbortDistributedTransactionPhaseTwo* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto error = FromProto<TError>(request->error());

        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            YT_LOG_ERROR(
                "Requested to execute phase two abort for a non-existing transaction; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        YT_VERIFY(commit->GetDistributed());
        YT_VERIFY(commit->GetPersistent());

        if (commit->GetPersistentState() != ECommitState::Prepare) {
            YT_LOG_ERROR(
                "Requested to execute phase two abort for transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        // Make a copy, commit may die.
        auto identity = commit->AuthenticationIdentity();
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        try {
            // Any exception thrown here is caught below.
            TTransactionAbortOptions options{
                .Force = true
            };
            TransactionManager_->AbortTransaction(transactionId, options);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error aborting transaction at coordinator; ignored (TransactionId: %v, State: %v, %v)",
                transactionId,
                ECommitState::Abort,
                NRpc::GetCurrentAuthenticationIdentity());
        }

        SetCommitFailed(commit, error);
        ChangeCommitPersistentState(commit, ECommitState::Abort);
        ChangeCommitTransientState(commit, ECommitState::Abort);

        YT_LOG_DEBUG("Coordinator aborted (TransactionId: %v, State: %v, %v)",
            transactionId,
            ECommitState::Abort,
            NRpc::GetCurrentAuthenticationIdentity());
    }

    void HydraCoordinatorAbortTransaction(NTransactionSupervisor::NProto::TReqCoordinatorAbortTransaction* request)
    {
        auto mutationId = FromProto<TMutationId>(request->mutation_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto force = request->force();

        auto* abort = FindAbort(transactionId);
        if (!abort) {
            abort = CreateAbort(transactionId, mutationId);
        }

        try {
            // All exceptions thrown here are caught below.
            TTransactionAbortOptions options{
                .Force = force
            };
            TransactionManager_->AbortTransaction(transactionId, options);
        } catch (const std::exception& ex) {
            SetAbortFailed(abort, ex);
            RemoveAbort(abort);
            YT_LOG_DEBUG(ex, "Error aborting transaction; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        auto* commit = FindCommit(transactionId);
        if (commit) {
            auto error = TError("Transaction %v was aborted", transactionId);
            SetCommitFailed(commit, error);

            if (commit->GetPersistent()) {
                ChangeCommitTransientState(commit, ECommitState::Abort);
                ChangeCommitPersistentState(commit, ECommitState::Abort);
            } else {
                RemoveTransientCommit(commit);
            }
        }

        SetAbortSucceeded(abort);
        RemoveAbort(abort);
    }

    void HydraCoordinatorFinishDistributedTransaction(NTransactionSupervisor::NProto::TReqCoordinatorFinishDistributedTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            YT_LOG_DEBUG("Requested to finish a non-existing transaction commit; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        // TODO(babenko): think about a better way of distinguishing between successful and failed commits
        if (commit->GetCoordinatorCommitMode() == ETransactionCoordinatorCommitMode::Lazy &&
            !commit->CommitTimestamps().Timestamps.empty())
        {
            RunCoordinatorCommit(commit);
        }

        RemovePersistentCommit(commit);

        // Transaction may have been (unsuccessfully) aborted. Cached abort errors should not outlive the commit.
        TryRemoveAbort(transactionId);

        YT_LOG_DEBUG("Distributed transaction commit finished (TransactionId: %v)",
            transactionId);
    }

    void HydraParticipantPrepareTransaction(NTransactionSupervisor::NProto::TReqParticipantPrepareTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prepareTimestamp = request->prepare_timestamp();
        auto prepareTimestampClusterTag = FromProto<TClusterTag>(request->prepare_timestamp_cluster_tag());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        try {
            // Any exception thrown here is caught below.
            TTransactionPrepareOptions options{
                .Persistent = true,
                .PrepareTimestamp = prepareTimestamp,
                .PrepareTimestampClusterTag = prepareTimestampClusterTag,
            };
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Participant failure (TransactionId: %v, State: %v, %v)",
                transactionId,
                ECommitState::Prepare,
                NRpc::GetCurrentAuthenticationIdentity());
            throw;
        }

        YT_LOG_DEBUG("Participant success (TransactionId: %v, State: %v, %v)",
            transactionId,
            ECommitState::Prepare,
            NRpc::GetCurrentAuthenticationIdentity());
    }

    void HydraParticipantCommitTransaction(NTransactionSupervisor::NProto::TReqParticipantCommitTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamp = request->commit_timestamp();
        auto commitTimestampClusterTag = FromProto<TClusterTag>(request->commit_timestamp_cluster_tag());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        try {
            // Any exception thrown here is caught below.
            TTransactionCommitOptions options{
                .CommitTimestamp = commitTimestamp,
                .CommitTimestampClusterTag = commitTimestampClusterTag
            };
            TransactionManager_->CommitTransaction(transactionId, options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Participant failure (TransactionId: %v, State: %v, %v)",
                transactionId,
                ECommitState::Commit,
                NRpc::GetCurrentAuthenticationIdentity());
            throw;
        }

        YT_LOG_DEBUG("Participant success (TransactionId: %v, State: %v, %v)",
            transactionId,
            ECommitState::Commit,
            NRpc::GetCurrentAuthenticationIdentity());
    }

    void HydraParticipantAbortTransaction(NTransactionSupervisor::NProto::TReqParticipantAbortTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        try {
            // Any exception thrown here is caught below.
            TTransactionAbortOptions options{
                .Force = true
            };
            TransactionManager_->AbortTransaction(transactionId, options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Participant failure (TransactionId: %v, State: %v, %v)",
                transactionId,
                ECommitState::Abort,
                NRpc::GetCurrentAuthenticationIdentity());
            throw;
        }

        YT_LOG_DEBUG("Participant success (TransactionId: %v, State: %v, %v)",
            transactionId,
            ECommitState::Abort,
            NRpc::GetCurrentAuthenticationIdentity());
    }


    TCommit* FindTransientCommit(TTransactionId transactionId)
    {
        return TransientCommitMap_.Find(transactionId);
    }

    TCommit* FindPersistentCommit(TTransactionId transactionId)
    {
        return PersistentCommitMap_.Find(transactionId);
    }

    TCommit* FindCommit(TTransactionId transactionId)
    {
        if (auto* commit = FindTransientCommit(transactionId)) {
            return commit;
        }
        if (auto* commit = FindPersistentCommit(transactionId)) {
            return commit;
        }
        return nullptr;
    }

    TCommit* CreateTransientCommit(
        TTransactionId transactionId,
        TMutationId mutationId,
        std::vector<TCellId> participantCellIds,
        std::vector<TCellId> prepareOnlyParticipantCellIds,
        std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
        bool distributed,
        bool generatePrepareTimestamp,
        bool inheritCommitTimestamp,
        ETransactionCoordinatorPrepareMode coordinatorPrepareMode,
        ETransactionCoordinatorCommitMode coordinatorCommitMode,
        TTimestamp maxAllowedCommitTimestamp,
        NRpc::TAuthenticationIdentity identity,
        std::vector<TTransactionId> prerequisiteTransactionIds)
    {
        auto commitHolder = std::make_unique<TCommit>(
            transactionId,
            mutationId,
            std::move(participantCellIds),
            std::move(prepareOnlyParticipantCellIds),
            std::move(cellIdsToSyncWithBeforePrepare),
            distributed,
            generatePrepareTimestamp,
            inheritCommitTimestamp,
            coordinatorPrepareMode,
            coordinatorCommitMode,
            maxAllowedCommitTimestamp,
            std::move(identity),
            std::move(prerequisiteTransactionIds));
        return TransientCommitMap_.Insert(transactionId, std::move(commitHolder));
    }

    TCommit* GetOrCreatePersistentCommit(
        TTransactionId transactionId,
        TMutationId mutationId,
        std::vector<TCellId> participantCellIds,
        std::vector<TCellId> prepareOnlyParticipantCellIds,
        std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
        bool distributed,
        bool generatePrepareTimestamp,
        bool inheritCommitTimestamp,
        ETransactionCoordinatorPrepareMode coordinatorPrepareMode,
        ETransactionCoordinatorCommitMode coordinatorCommitMode,
        TTimestamp maxAllowedCommitTimestamp,
        NRpc::TAuthenticationIdentity identity)
    {
        if (Decommissioned_) {
            THROW_ERROR_EXCEPTION("Tablet cell %v is decommissioned",
                SelfCellId_);
        }

        auto* commit = FindCommit(transactionId);
        std::unique_ptr<TCommit> commitHolder;
        if (commit) {
            YT_VERIFY(!commit->GetPersistent());
            commitHolder = TransientCommitMap_.Release(transactionId);
        } else {
            commitHolder = std::make_unique<TCommit>(
                transactionId,
                mutationId,
                std::move(participantCellIds),
                std::move(prepareOnlyParticipantCellIds),
                std::move(cellIdsToSyncWithBeforePrepare),
                distributed,
                generatePrepareTimestamp,
                inheritCommitTimestamp,
                coordinatorPrepareMode,
                coordinatorCommitMode,
                maxAllowedCommitTimestamp,
                std::move(identity));
        }
        commitHolder->SetPersistent(true);
        return PersistentCommitMap_.Insert(transactionId, std::move(commitHolder));
    }


    void RemoveTransientCommit(TCommit* commit)
    {
        YT_VERIFY(!commit->GetPersistent());
        TransientCommitMap_.Remove(commit->GetTransactionId());
    }

    void RemovePersistentCommit(TCommit* commit)
    {
        YT_VERIFY(commit->GetPersistent());
        PersistentCommitMap_.Remove(commit->GetTransactionId());
    }


    void SetCommitFailed(TCommit* commit, const TError& error)
    {
        YT_LOG_DEBUG(error, "Transaction commit failed (TransactionId: %v)",
            commit->GetTransactionId());

        auto responseMessage = CreateErrorResponseMessage(error);
        SetCommitResponse(commit, responseMessage, /*remember*/ false);
    }

    void SetCommitSucceeded(TCommit* commit)
    {
        YT_LOG_DEBUG("Transaction commit succeeded (TransactionId: %v, CommitTimestamps: %v)",
            commit->GetTransactionId(),
            commit->CommitTimestamps());

        NProto::NTransactionSupervisor::TRspCommitTransaction response;
        ToProto(response.mutable_commit_timestamps(), commit->CommitTimestamps());

        auto responseMessage = CreateResponseMessage(response);
        SetCommitResponse(commit, std::move(responseMessage));
    }

    void SetCommitResponse(TCommit* commit, TSharedRefArray responseMessage, bool remember = true)
    {
        if (auto mutationId = commit->GetMutationId()) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage, remember);
        }

        commit->SetResponseMessage(std::move(responseMessage));
    }


    bool RunCoordinatorPrepare(TCommit* commit)
    {
        YT_VERIFY(HasMutationContext());

        auto transactionId = commit->GetTransactionId();
        auto prepareMode = commit->GetCoordinatorPrepareMode();
        auto latePrepare = prepareMode == ETransactionCoordinatorPrepareMode::Late;

        YT_LOG_DEBUG(
            "Preparing at coordinator (TransactionId: %v, PrepareTimestamp: %v@%v, PrepareMode: %v)",
            transactionId,
            commit->PrepareTimestamp(),
            commit->PrepareTimestampClusterTag(),
            prepareMode);

        try {
            // Any exception thrown here is caught below.
            const auto& prerequisiteTransactionIds = commit->PrerequisiteTransactionIds();
            YT_VERIFY(prerequisiteTransactionIds.empty());

            TTransactionPrepareOptions options{
                .Persistent = true,
                .LatePrepare = latePrepare,
                .PrepareTimestamp = commit->PrepareTimestamp(),
                .PrepareTimestampClusterTag = commit->PrepareTimestampClusterTag(),
                .PrerequisiteTransactionIds = prerequisiteTransactionIds,
            };
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Coordinator failure; will abort (TransactionId: %v, State: %v, %v)",
                transactionId,
                ECommitState::Prepare,
                NRpc::GetCurrentAuthenticationIdentity());

            // If prepare mode is late, we have to abort transaction at all participants.
            // For early prepare mode it is sufficient to abort at coordinator only.
            if (latePrepare) {
                // COMPAT(gritukan)
                // Currently, transactions with late prepare are coordinated by master only
                // and rolling update of abort semantics change is not possible.
                auto reign = GetCurrentMutationContext()->Request().Reign;
                // ETabletReign::LockingState = 100700.
                YT_VERIFY(reign <= 3000 || (reign >= 100700 && reign < 103000));

                auto error = TError(
                    NTransactionClient::EErrorCode::ParticipantFailedToPrepare,
                    "Coordinator has failed to prepare")
                    << ex;
                ChangeCommitTransientState(commit, ECommitState::Aborting, error);
                return false;
            }

            SetCommitFailed(commit, ex);
            RemovePersistentCommit(commit);
            try {
                TTransactionAbortOptions options{
                    .Force = true
                };
                TransactionManager_->AbortTransaction(transactionId, options);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error aborting transaction at coordinator; ignored (TransactionId: %v, %v)",
                    transactionId,
                    NRpc::GetCurrentAuthenticationIdentity());
            }
            return false;
        }

        YT_LOG_DEBUG(
            "Coordinator prepared (TransactionId: %v)",
            transactionId);

        return true;
    }

    void RunCoordinatorCommit(TCommit* commit)
    {
        YT_VERIFY(HasMutationContext());

        // Make a copy, commit may die.
        auto identity = commit->AuthenticationIdentity();
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto transactionId = commit->GetTransactionId();
        SetCommitSucceeded(commit);

        try {
            // Any exception thrown here is caught below.
            auto commitTimestamp = commit->CommitTimestamps().GetTimestamp(CellTagFromId(SelfCellId_));
            TTransactionCommitOptions options{
                .CommitTimestamp = commitTimestamp,
                .CommitTimestampClusterTag = SelfClockClusterTag_
            };
            TransactionManager_->CommitTransaction(transactionId, options);

            YT_LOG_DEBUG("Coordinator success (TransactionId: %v, State: %v, %v)",
                transactionId,
                commit->GetPersistentState(),
                NRpc::GetCurrentAuthenticationIdentity());
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Coordinator failure; ignored (TransactionId: %v, State: %v, %v)",
                transactionId,
                commit->GetPersistentState(),
                NRpc::GetCurrentAuthenticationIdentity());
        }
    }


    TAbort* FindAbort(TTransactionId transactionId)
    {
        auto it = TransientAbortMap_.find(transactionId);
        return it == TransientAbortMap_.end() ? nullptr : &it->second;
    }

    TAbort* CreateAbort(TTransactionId transactionId, TMutationId mutationId)
    {
        auto pair = TransientAbortMap_.emplace(transactionId, TAbort(transactionId, mutationId));
        YT_VERIFY(pair.second);
        return &pair.first->second;
    }

    void SetAbortFailed(TAbort* abort, const TError& error)
    {
        YT_LOG_DEBUG(error, "Transaction abort failed (TransactionId: %v)",
            abort->GetTransactionId());

        auto responseMessage = CreateErrorResponseMessage(error);
        SetAbortResponse(abort, std::move(responseMessage), /*remember*/ false);
    }

    void SetAbortSucceeded(TAbort* abort)
    {
        YT_LOG_DEBUG("Transaction abort succeeded (TransactionId: %v)",
            abort->GetTransactionId());

        NProto::NTransactionSupervisor::TRspAbortTransaction response;

        auto responseMessage = CreateResponseMessage(response);
        SetAbortResponse(abort, std::move(responseMessage));
    }

    void SetAbortResponse(TAbort* abort, TSharedRefArray responseMessage, bool remember = true)
    {
        auto mutationId = abort->GetMutationId();
        if (mutationId) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage, remember);
        }

        abort->SetResponseMessage(std::move(responseMessage));
    }

    void RemoveAbort(TAbort* abort)
    {
        YT_VERIFY(TransientAbortMap_.erase(abort->GetTransactionId()) == 1);
    }

    void TryRemoveAbort(TTransactionId transactionId)
    {
        TransientAbortMap_.erase(transactionId);
    }


    void GenerateCommitTimestamps(TCommit* commit)
    {
        auto transactionId = commit->GetTransactionId();

        TFuture<TTimestamp> asyncCoordinatorTimestamp;
        std::vector<TFuture<std::pair<TCellTag, TTimestamp>>> asyncTimestamps;
        THashSet<TCellTag> timestampProviderCellTags;
        auto generateFor = [&] (TCellId cellId) {
            try {
                auto cellTag = CellTagFromId(cellId);
                if (!timestampProviderCellTags.insert(cellTag).second) {
                    return;
                }

                auto participant = GetParticipant(cellId);
                auto timestampProvider = participant->GetTimestampProviderOrThrow();

                TFuture<TTimestamp> asyncTimestamp;
                if (commit->GetInheritCommitTimestamp() && cellId != SelfCellId_) {
                    YT_LOG_DEBUG("Inheriting commit timestamp (TransactionId: %v, ParticipantCellId: %v)",
                        transactionId,
                        cellId);
                    YT_VERIFY(asyncCoordinatorTimestamp);
                    asyncTimestamp = asyncCoordinatorTimestamp;
                } else {
                    YT_LOG_DEBUG("Generating commit timestamp (TransactionId: %v, ParticipantCellId: %v)",
                        transactionId,
                        cellId);
                    asyncTimestamp = TimestampProvider_->GenerateTimestamps(1);
                }
                asyncTimestamps.push_back(asyncTimestamp.Apply(BIND([=] (TTimestamp timestamp) {
                    return std::make_pair(cellTag, timestamp);
                })));
                if (cellId == SelfCellId_ && !asyncCoordinatorTimestamp) {
                    asyncCoordinatorTimestamp = asyncTimestamp;
                }
            } catch (const std::exception& ex) {
                asyncTimestamps.push_back(MakeFuture<std::pair<TCellTag, TTimestamp>>(ex));
            }
        };

        generateFor(SelfCellId_);
        for (auto cellId : commit->ParticipantCellIds()) {
            generateFor(cellId);
        }

        AllSucceeded(asyncTimestamps)
            .Subscribe(BIND(&TTransactionSupervisor::OnCommitTimestampsGenerated, MakeStrong(this), transactionId)
                .Via(EpochAutomatonInvoker_));
    }

    void OnCommitTimestampsGenerated(
        TTransactionId transactionId,
        const TErrorOr<std::vector<std::pair<TCellTag, TTimestamp>>>& timestampsOrError)
    {
        auto* commit = FindCommit(transactionId);
        if (!commit) {
            YT_LOG_DEBUG("Commit timestamp generated for a non-existing transaction commit; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        if (!timestampsOrError.IsOK()) {
            // If this is a distributed transaction then it's already prepared at coordinator and
            // at all participants. We _must_ forcefully abort it.
            YT_LOG_DEBUG(timestampsOrError, "Error generating commit timestamps (TransactionId: %v)",
                transactionId);
            YT_UNUSED_FUTURE(AbortTransaction(transactionId, true));
            return;
        }

        const auto& result = timestampsOrError.Value();

        TTimestampMap commitTimestamps;
        commitTimestamps.Timestamps.insert(commitTimestamps.Timestamps.end(), result.begin(), result.end());

        YT_LOG_DEBUG("Commit timestamps generated (TransactionId: %v, CommitTimestamps: %v)",
            transactionId,
            commitTimestamps);

        if (auto maxAllowedCommitTimestamp = commit->GetMaxAllowedCommitTimestamp()) {
            for (auto [cellTag, commitTimestamp] : result) {
                if (commitTimestamp > maxAllowedCommitTimestamp) {
                    YT_LOG_DEBUG("Generated commit timestamp exceeds max allowed commit timestamp "
                        "(TransactionId: %v, CellTag: %v, CommitTimestamp: %v, MaxAllowedCommitTimestamp: %v)",
                        transactionId,
                        cellTag,
                        commitTimestamp,
                        maxAllowedCommitTimestamp);
                    YT_UNUSED_FUTURE(AbortTransaction(transactionId, true));
                    return;
                }
            }
        }

        if (commit->GetDistributed()) {
            NTransactionSupervisor::NProto::TReqCoordinatorCommitDistributedTransactionPhaseTwo request;
            ToProto(request.mutable_transaction_id(), transactionId);
            ToProto(request.mutable_commit_timestamps(), commitTimestamps);

            auto mutation = CreateMutation(HydraManager_, request);
            mutation->SetCurrentTraceContext();
            YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
        } else {
            NTransactionSupervisor::NProto::TReqCoordinatorCommitSimpleTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            ToProto(request.mutable_mutation_id(), commit->GetMutationId());
            ToProto(request.mutable_commit_timestamps(), commitTimestamps);
            WriteAuthenticationIdentityToProto(&request, commit->AuthenticationIdentity());

            auto mutation = CreateMutation(HydraManager_, request);
            mutation->SetCurrentTraceContext();
            YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
        }
    }


    TWrappedParticipantPtr GetParticipant(TCellId cellId)
    {
        TWrappedParticipantPtr participant;
        if (auto it = ParticipantMap_.find(cellId)) {
            participant = it->second;
            if (participant->GetState() == ETransactionParticipantState::Invalidated) {
                YT_LOG_DEBUG("Invalidated participant unregistered (ParticipantCellId: %v)",
                    cellId);
                ParticipantMap_.erase(it);
                participant.Reset();
            }
        }

        if (!participant) {
            participant = New<TWrappedParticipant>(
                cellId,
                Config_,
                TimestampProvider_,
                SelfClockClusterTag_,
                ParticipantProviders_,
                Logger);
            YT_VERIFY(ParticipantMap_.emplace(cellId, participant).second);
            YT_LOG_DEBUG("Participant registered (ParticipantCellId: %v)",
                cellId);
        }

        participant->Touch();

        return participant;
    }

    void OnParticipantCleanup()
    {
        for (auto it = ParticipantMap_.begin(); it != ParticipantMap_.end(); ) {
            auto jt = it++;
            const auto& participant = jt->second;
            if (participant->IsExpired()) {
                YT_LOG_DEBUG("Participant expired (ParticipantCellId: %v, State: %v)",
                    participant->GetCellId(),
                    participant->GetState());
                ParticipantMap_.erase(jt);
            }
        }
    }


    void ChangeCommitTransientState(TCommit* commit, ECommitState state, const TError& error = TError())
    {
        if (!IsLeader()) {
            return;
        }

        YT_LOG_DEBUG("Commit transient state changed (TransactionId: %v, State: %v -> %v)",
            commit->GetTransactionId(),
            commit->GetTransientState(),
            state);
        commit->SetTransientState(state);
        commit->RespondedCellIds().clear();

        switch (state) {
            case ECommitState::GeneratingCommitTimestamps:
                GenerateCommitTimestamps(commit);
                break;

            case ECommitState::Prepare:
            case ECommitState::Commit:
            case ECommitState::Abort:
                SendParticipantRequests(commit);
                break;

            case ECommitState::Aborting: {
                NTransactionSupervisor::NProto::TReqCoordinatorAbortDistributedTransactionPhaseTwo request;
                ToProto(request.mutable_transaction_id(), commit->GetTransactionId());
                ToProto(request.mutable_error(), error);

                auto mutation = CreateMutation(HydraManager_, request);
                mutation->SetCurrentTraceContext();
                YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
                break;
            }

            case ECommitState::Finishing: {
                NTransactionSupervisor::NProto::TReqCoordinatorFinishDistributedTransaction request;
                ToProto(request.mutable_transaction_id(), commit->GetTransactionId());

                auto mutation = CreateMutation(HydraManager_, request);
                mutation->SetCurrentTraceContext();
                YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void ChangeCommitPersistentState(TCommit* commit, ECommitState state)
    {
        YT_LOG_DEBUG("Commit persistent state changed (TransactionId: %v, State: %v -> %v)",
            commit->GetTransactionId(),
            commit->GetPersistentState(),
            state);
        commit->SetPersistentState(state);
    }

    void SendParticipantRequests(TCommit* commit)
    {
        YT_VERIFY(commit->RespondedCellIds().empty());
        for (auto cellId : commit->ParticipantCellIds()) {
            SendParticipantRequest(commit, cellId);
        }
        CheckAllParticipantsResponded(commit);
    }

    void SendParticipantRequest(TCommit* commit, TCellId cellId)
    {
        auto state = commit->GetTransientState();
        if (state != ECommitState::Prepare && commit->IsPrepareOnlyParticipant(cellId)) {
            commit->RespondedCellIds().insert(cellId);
            YT_LOG_DEBUG("Omitting participant request (TransactionId: %v, ParticipantCellId: %v, State: %v)",
                commit->GetTransactionId(),
                cellId,
                state);
            return;
        }

        auto participant = GetParticipant(cellId);

        TFuture<void> response;
        switch (state) {
            case ECommitState::Prepare:
                response = participant->PrepareTransaction(commit);
                break;

            case ECommitState::Commit:
                response = participant->CommitTransaction(commit);
                break;

            case ECommitState::Abort:
                response = participant->AbortTransaction(commit);
                break;

            default:
                YT_ABORT();
        }
        response.Subscribe(
            BIND(&TTransactionSupervisor::OnParticipantResponse, MakeWeak(this), commit->GetTransactionId(), state, participant)
                .Via(EpochAutomatonInvoker_));
    }

    bool IsParticipantResponseSuccessful(
        TCommit* commit,
        const TWrappedParticipantPtr& participant,
        const TError& error)
    {
        if (error.IsOK()) {
            return true;
        }

        if (error.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction) &&
            commit->GetTransientState() != ECommitState::Prepare)
        {
            YT_LOG_DEBUG("Transaction is missing at participant; still consider this a success "
                "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                commit->GetTransactionId(),
                participant->GetCellId(),
                commit->GetTransientState());
            return true;
        }

        return false;
    }

    bool IsParticipantUp(const TError& error)
    {
        return !IsRetriableError(error);
    }

    void OnParticipantResponse(
        TTransactionId transactionId,
        ECommitState state,
        const TWrappedParticipantPtr& participant,
        const TError& error)
    {
        if (IsParticipantUp(error)) {
            participant->SetUp();
        } else {
            participant->SetDown(error);
        }

        const auto& participantCellId = participant->GetCellId();

        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            YT_LOG_DEBUG("Received participant response for a non-existing commit; ignored (TransactionId: %v, ParticipantCellId: %v)",
                transactionId,
                participantCellId);
            return;
        }

        if (state != commit->GetTransientState()) {
            YT_LOG_DEBUG("Received participant response for a commit in wrong state; ignored (TransactionId: %v, "
                "ParticipantCellId: %v, ExpectedState: %v, ActualState: %v)",
                transactionId,
                participantCellId,
                state,
                commit->GetTransientState());
            return;
        }

        if (IsParticipantResponseSuccessful(commit, participant, error)) {
            YT_LOG_DEBUG("Coordinator observes participant success "
                "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                commit->GetTransactionId(),
                participantCellId,
                state);

            // NB: Duplicates are fine.
            commit->RespondedCellIds().insert(participantCellId);
            CheckAllParticipantsResponded(commit);
        } else {
            switch (state) {
                case ECommitState::Prepare: {
                    YT_LOG_DEBUG(error, "Coordinator observes participant failure; will abort "
                        "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                        commit->GetTransactionId(),
                        participantCellId,
                        state);
                    auto wrappedError = TError(
                        NTransactionClient::EErrorCode::ParticipantFailedToPrepare,
                        "Participant %v has failed to prepare",
                        participantCellId)
                        << error;
                    ChangeCommitTransientState(commit, ECommitState::Aborting, wrappedError);
                    break;
                }

                case ECommitState::Commit:
                case ECommitState::Abort:
                    YT_LOG_DEBUG(error, "Coordinator observes participant failure; will retry "
                        "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                        commit->GetTransactionId(),
                        participantCellId,
                        state);
                    SendParticipantRequest(commit, participantCellId);
                    break;

                default:
                    YT_LOG_DEBUG(error, "Coordinator observes participant failure; ignored "
                        "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                        commit->GetTransactionId(),
                        participantCellId,
                        state);
                    break;
            }
            return;
        }
    }

    void CheckAllParticipantsResponded(TCommit* commit)
    {
        if (commit->RespondedCellIds().size() == commit->ParticipantCellIds().size()) {
            ChangeCommitTransientState(commit, GetNewCommitState(commit->GetTransientState()));
        }
    }


    static ECommitState GetNewCommitState(ECommitState state)
    {
        switch (state) {
            case ECommitState::Prepare:
                return ECommitState::GeneratingCommitTimestamps;

            case ECommitState::GeneratingCommitTimestamps:
                return ECommitState::Commit;

            case ECommitState::Commit:
            case ECommitState::Abort:
                return ECommitState::Finishing;

            default:
                YT_ABORT();
        }
    }


    bool ValidateSnapshotVersion(int version) override
    {
        return
            version == 10 || // babenko: YTINCIDENTS-56: Add CellIdsToSyncWithBeforePrepare
            version == 11 || // ifsmirnov: YT-15025: MaxAllowedCommitTimestamp
            version == 12 || // gritukan: YT-16858: Coordinator prepare mode.
            version == 13 || // gritukan: Abort failed simple transactions.
            false;
    }

    int GetCurrentSnapshotVersion() override
    {
        return 13;
    }


    void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderActive();

        ParticipantCleanupExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND(&TTransactionSupervisor::OnParticipantCleanup, MakeWeak(this)),
            ParticipantCleanupPeriod);
        YT_UNUSED_FUTURE(ParticipantCleanupExecutor_->Stop());

        YT_VERIFY(TransientCommitMap_.GetSize() == 0);
        for (auto [transactionId, commit] : PersistentCommitMap_) {
            ChangeCommitTransientState(commit, commit->GetPersistentState());
        }
    }

    void OnStopLeading() override
    {
        TCompositeAutomatonPart::OnStopLeading();

        if (ParticipantCleanupExecutor_) {
            YT_UNUSED_FUTURE(ParticipantCleanupExecutor_->Stop());
        }
        ParticipantCleanupExecutor_.Reset();

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");

        for (auto [transactionId, commit] : TransientCommitMap_) {
            SetCommitFailed(commit, error);
        }
        TransientCommitMap_.Clear();

        for (auto& [transactionId, abort] : TransientAbortMap_) {
            SetAbortFailed(&abort, error);
        }
        TransientAbortMap_.clear();

        TransientCommitMap_.Clear();
        ParticipantMap_.clear();
    }


    void Clear() override
    {
        TCompositeAutomatonPart::Clear();

        PersistentCommitMap_.Clear();
        TransientCommitMap_.Clear();
        TransientAbortMap_.clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        PersistentCommitMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        PersistentCommitMap_.SaveValues(context);
        Save(context, Decommissioned_);
    }

    void LoadKeys(TLoadContext& context)
    {
        PersistentCommitMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        PersistentCommitMap_.LoadValues(context);
        Load(context, Decommissioned_);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisor)

////////////////////////////////////////////////////////////////////////////////

ITransactionSupervisorPtr CreateTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IInvokerPtr trackerInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IResponseKeeperPtr responseKeeper,
    ITransactionManagerPtr transactionManager,
    TCellId selfCellId,
    TClusterTag selfClockClusterTag,
    ITimestampProviderPtr timestampProvider,
    std::vector<ITransactionParticipantProviderPtr> participantProviders,
    IAuthenticatorPtr authenticator)
{
    return New<TTransactionSupervisor>(
        std::move(config),
        std::move(automatonInvoker),
        std::move(trackerInvoker),
        std::move(hydraManager),
        std::move(automaton),
        std::move(responseKeeper),
        std::move(transactionManager),
        selfCellId,
        selfClockClusterTag,
        std::move(timestampProvider),
        std::move(participantProviders),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
