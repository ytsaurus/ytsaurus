#include "transaction_supervisor.h"
#include "commit.h"
#include "abort.h"
#include "config.h"
#include "transaction_manager.h"
#include "transaction_participant_provider.h"
#include "private.h"

#include <yt/server/hive/transaction_supervisor.pb.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/entity_map.h>
#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/hydra_service.h>
#include <yt/server/hydra/mutation.h>

#include <yt/ytlib/hive/transaction_supervisor_service_proxy.h>
#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/transaction_participant.h>
#include <yt/ytlib/hive/transaction_participant_service_proxy.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>
#include <yt/ytlib/transaction_client/action.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/api/connection.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/proto/rpc.pb.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NHiveServer {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra;
using namespace NHiveClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto ParticipantCleanupPeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor::TImpl
    : public TCompositeAutomatonPart
{
public:
    TImpl(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        IInvokerPtr trackerInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        TResponseKeeperPtr responseKeeper,
        ITransactionManagerPtr transactionManager,
        const TCellId& selfCellId,
        ITimestampProviderPtr timestampProvider,
        const std::vector<ITransactionParticipantProviderPtr>& participantProviders)
        : TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , Config_(config)
        , TrackerInvoker_(trackerInvoker)
        , HydraManager_(hydraManager)
        , ResponseKeeper_(responseKeeper)
        , TransactionManager_(transactionManager)
        , SelfCellId_(selfCellId)
        , TimestampProvider_(timestampProvider)
        , ParticipantProviders_(participantProviders)
        , Logger(NLogging::TLogger(HiveServerLogger)
            .AddTag("CellId: %v", SelfCellId_))
        , TransactionSupervisorService_(New<TTransactionSupervisorService>(this))
        , TransactionParticipantService_(New<TTransactionParticipantService>(this))
    {
        YCHECK(Config_);
        YCHECK(TrackerInvoker_);
        YCHECK(ResponseKeeper_);
        YCHECK(TransactionManager_);
        YCHECK(TimestampProvider_);

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorCommitSimpleTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorCommitDistributedTransactionPhaseOne, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorCommitDistributedTransactionPhaseTwo, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorAbortDistributedTransactionPhaseTwo, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorAbortTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCoordinatorFinishDistributedTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraParticipantPrepareTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraParticipantCommitTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraParticipantAbortTransaction, Unretained(this)));

        RegisterLoader(
            "TransactionSupervisor.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionSupervisor.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionSupervisor.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionSupervisor.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    std::vector<IServicePtr> GetRpcServices()
    {
        return std::vector<IServicePtr>{
            TransactionSupervisorService_,
            TransactionParticipantService_
        };
    }

    TFuture<void> CommitTransaction(
        const TTransactionId& transactionId,
        const std::vector<TCellId>& participantCellIds)
    {
        return MessageToError(
            CoordinatorCommitTransaction(
                transactionId,
                participantCellIds,
                false,
                false,
                NullMutationId));
    }

    TFuture<void> AbortTransaction(
        const TTransactionId& transactionId,
        bool force)
    {
        return MessageToError(
            CoordinatorAbortTransaction(
                transactionId,
                NullMutationId,
                force));
    }

private:
    const TTransactionSupervisorConfigPtr Config_;
    const IInvokerPtr TrackerInvoker_;
    const IHydraManagerPtr HydraManager_;
    const TResponseKeeperPtr ResponseKeeper_;
    const ITransactionManagerPtr TransactionManager_;
    const TCellId SelfCellId_;
    const ITimestampProviderPtr TimestampProvider_;
    const std::vector<ITransactionParticipantProviderPtr> ParticipantProviders_;

    const NLogging::TLogger Logger;

    TEntityMap<TCommit> TransientCommitMap_;
    TEntityMap<TCommit> PersistentCommitMap_;

    yhash<TTransactionId, TAbort> TransientAbortMap_;


    class TWrappedParticipant
        : public TRefCounted
    {
    public:
        TWrappedParticipant(
            const TCellId& cellId,
            TTransactionSupervisorConfigPtr config,
            ITimestampProviderPtr coordinatorTimestampProvider,
            const std::vector<ITransactionParticipantProviderPtr>& providers,
            const NLogging::TLogger logger)
            : CellId_(cellId)
            , Config_(std::move(config))
            , CoordinatorTimestampProvider_(std::move(coordinatorTimestampProvider))
            , Providers_(providers)
            , ProbationExecutor_(New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetLightInvoker(),
                BIND(&TWrappedParticipant::OnProbation, MakeWeak(this)),
                Config_->ParticipantProbationPeriod))
            , Logger(NLogging::TLogger(logger)
                .AddTag("ParticipantCellId: %v", CellId_))
        {
            ProbationExecutor_->Start();
        }

        const TCellId& GetCellId() const
        {
            return CellId_;
        }

        bool IsValid()
        {
            auto guard = Guard(SpinLock_);
            auto underlying = GetUnderlying();
            return !underlying || underlying->IsValid();
        }

        ITimestampProviderPtr GetTimestampProviderOrThrow()
        {
            auto guard = Guard(SpinLock_);

            auto underlying = GetUnderlying();
            if (!underlying) {
                THROW_ERROR MakeUnavailbleError();
            }

            return underlying->GetTimestampProvider();
        }

        TFuture<void> PrepareTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                false,
                true,
                [
                    transactionId = commit->GetTransactionId(),
                    inheritCommitTimestamp = commit->GetInheritCommitTimestamp(),
                    coordinatorTimestampProvider = CoordinatorTimestampProvider_
                ]
                (const ITransactionParticipantPtr& participant) {
                    const auto& timestampProvider = inheritCommitTimestamp
                        ? coordinatorTimestampProvider
                        : participant->GetTimestampProvider();
                    auto prepareTimestamp = timestampProvider->GetLatestTimestamp();
                    return participant->PrepareTransaction(transactionId, prepareTimestamp);
                });
        }

        TFuture<void> CommitTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                true,
                false,
                [transactionId = commit->GetTransactionId(), commitTimestamps = commit->CommitTimestamps()]
                (const ITransactionParticipantPtr& participant) {
                    auto cellTag = CellTagFromId(participant->GetCellId());
                    auto commitTimestamp = commitTimestamps.GetTimestamp(cellTag);
                    return participant->CommitTransaction(transactionId, commitTimestamp);
                });
        }

        TFuture<void> AbortTransaction(TCommit* commit)
        {
            return EnqueueRequest(
                true,
                false,
                [transactionId = commit->GetTransactionId()]
                (const ITransactionParticipantPtr& participant) {
                    return participant->AbortTransaction(transactionId);
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

            LOG_DEBUG("Participant cell is up");

            for (const auto& sender : senders) {
                sender.Run();
            }
        }

        void SetDown(const TError& error)
        {
            auto guard = Guard(SpinLock_);

            if (!Up_) {
                return;
            }

            Up_ = false;

            LOG_DEBUG(error, "Participant cell is down");
        }

    private:
        const TCellId CellId_;
        const TTransactionSupervisorConfigPtr Config_;
        const ITimestampProviderPtr CoordinatorTimestampProvider_;
        const std::vector<ITransactionParticipantProviderPtr> Providers_;
        const TPeriodicExecutorPtr ProbationExecutor_;
        const NLogging::TLogger Logger;

        TSpinLock SpinLock_;
        ITransactionParticipantPtr Underlying_;
        std::vector<TClosure> PendingSenders_;
        bool Up_ = true;


        ITransactionParticipantPtr GetUnderlying()
        {
            if (!Underlying_) {
                Underlying_ = TryCreateUnderlying();
            }
            return Underlying_;
        }

        ITransactionParticipantPtr TryCreateUnderlying()
        {
            TTransactionParticipantOptions options;
            options.RpcTimeout = Config_->RpcTimeout;

            for (const auto& provider : Providers_) {
                auto participant = provider->TryCreate(CellId_, options);
                if (participant) {
                    return participant;
                }
            }
            return nullptr;
        }

        template <class F>
        TFuture<void> EnqueueRequest(
            bool succeedOnInvalid,
            bool mustSendImmediately,
            F func)
        {
            auto promise = NewPromise<void>();

            auto guard = Guard(SpinLock_);

            auto underlying = GetUnderlying();

            if (!underlying) {
                return MakeFuture<void>(MakeUnavailbleError());
            }

            auto sender = BIND([=, underlying = std::move(underlying)] () mutable {
                if (underlying->IsValid()) {
                    promise.SetFrom(func(underlying));
                } else if (succeedOnInvalid) {
                    LOG_DEBUG("Transaction participant is no longer valid; assuming success");
                    promise.Set(TError());
                } else {
                    promise.Set(TError("Participant cell %v is no longer valid", CellId_));
                }
            });

            if (!TrySendRequestImmediately(sender, &guard)) {
                if (mustSendImmediately) {
                    return MakeFuture<void>(MakeDownError());
                }
                PendingSenders_.emplace_back(std::move(sender));
            }

            return promise;
        }

        bool TrySendRequestImmediately(const TClosure& sender, TGuard<TSpinLock>* guard)
        {
            LOG_DEBUG("XXX %v %v", Up_, PendingSenders_.size());
            if (!Up_ && !PendingSenders_.empty()) {
                return false;
            }

            guard->Release();
            sender.Run();
            return true;
        }

        void OnProbation()
        {
            auto guard = Guard(SpinLock_);

            if (Up_ || PendingSenders_.empty()) {
                return;
            }

            auto sender = PendingSenders_.back();
            PendingSenders_.pop_back();

            guard.Release();

            sender.Run();
        }

        TError MakeUnavailbleError() const
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
    };

    using TWrappedParticipantPtr = TIntrusivePtr<TWrappedParticipant>;
    using TWrappedParticipantWeakPtr = TWeakPtr<TWrappedParticipant>;

    yhash<TCellId, TWrappedParticipantPtr> StrongParticipantMap_;
    yhash<TCellId, TWrappedParticipantWeakPtr> WeakParticipantMap_;
    TPeriodicExecutorPtr ParticipantCleanupExecutor_;



    class TOwnedServiceBase
        : public THydraServiceBase
    {
    protected:
        explicit TOwnedServiceBase(
            TImplPtr owner,
            const TServiceDescriptor& descriptor)
            : THydraServiceBase(
                owner->HydraManager_->CreateGuardedAutomatonInvoker(owner->AutomatonInvoker_),
                descriptor,
                HiveServerLogger,
                owner->SelfCellId_)
            , Owner_(owner)
            , HydraManager_(owner->HydraManager_)
        { }

        TImplPtr GetOwnerOrThrow()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service is shutting down");
            }
            return owner;
        }

    private:
        const TWeakPtr<TImpl> Owner_;
        const IHydraManagerPtr HydraManager_;

        virtual IHydraManagerPtr GetHydraManager() override
        {
            return HydraManager_;
        }
    };


    class TTransactionSupervisorService
        : public TOwnedServiceBase
    {
    public:
        explicit TTransactionSupervisorService(TImplPtr owner)
            : TOwnedServiceBase(
                owner,
                TTransactionSupervisorServiceProxy::GetDescriptor())
        {
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction)
                .SetInvoker(owner->TrackerInvoker_));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionSupervisor, CommitTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto participantCellIds = FromProto<std::vector<TCellId>>(request->participant_cell_ids());
            auto force2PC = request->force_2pc();
            auto inheritCommitTimestamp = request->inherit_commit_timestamp();

            context->SetRequestInfo("TransactionId: %v, ParticipantCellIds: %v, Force2PC: %v, "
                "InheritCommitTimestamp: %v",
                transactionId,
                participantCellIds,
                force2PC,
                inheritCommitTimestamp);

            auto owner = GetOwnerOrThrow();

            if (owner->ResponseKeeper_->TryReplyFrom(context)) {
                return;
            }

            auto asyncResponseMessage = owner->CoordinatorCommitTransaction(
                transactionId,
                participantCellIds,
                force2PC,
                inheritCommitTimestamp,
                context->GetMutationId());
            context->ReplyFrom(asyncResponseMessage);
        }

        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionSupervisor, AbortTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            bool force = request->force();

            context->SetRequestInfo("TransactionId: %v, Force: %v",
                transactionId,
                force);

            auto owner = GetOwnerOrThrow();

            if (owner->ResponseKeeper_->TryReplyFrom(context)) {
                return;
            }

            auto asyncResponseMessage = owner->CoordinatorAbortTransaction(
                transactionId,
                context->GetMutationId(),
                force);
            context->ReplyFrom(asyncResponseMessage);
        }

        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionSupervisor, PingTransaction)
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
    };

    const TIntrusivePtr<TTransactionSupervisorService> TransactionSupervisorService_;


    class TTransactionParticipantService
        : public TOwnedServiceBase
    {
    public:
        explicit TTransactionParticipantService(TImplPtr owner)
            : TOwnedServiceBase(
                owner,
                TTransactionParticipantServiceProxy::GetDescriptor())
        {
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareTransaction));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
            TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionParticipant, PrepareTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto prepareTimestamp = request->prepare_timestamp();

            context->SetRequestInfo("TransactionId: %v, PrepareTimestamp: %llx",
                transactionId,
                prepareTimestamp);

            auto owner = GetOwnerOrThrow();
            NHiveServer::NProto::TReqParticipantPrepareTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_prepare_timestamp(prepareTimestamp);

            CreateMutation(owner->HydraManager_, hydraRequest)
                ->CommitAndReply(context);
        }

        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionParticipant, CommitTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());
            auto commitTimestamp = request->commit_timestamp();

            context->SetRequestInfo("TransactionId: %v, CommitTimestamp: %llx",
                transactionId,
                commitTimestamp);

            auto owner = GetOwnerOrThrow();
            NHiveServer::NProto::TReqParticipantCommitTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(commitTimestamp);

            CreateMutation(owner->HydraManager_, hydraRequest)
                ->CommitAndReply(context);
        }

        DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto::NTransactionParticipant, AbortTransaction)
        {
            ValidatePeer(EPeerKind::Leader);

            auto transactionId = FromProto<TTransactionId>(request->transaction_id());

            context->SetRequestInfo("TransactionId: %v",
                transactionId);

            auto owner = GetOwnerOrThrow();
            NHiveServer::NProto::TReqParticipantAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);

            CreateMutation(owner->HydraManager_, hydraRequest)
                ->CommitAndReply(context);
        }
    };

    const TIntrusivePtr<TTransactionParticipantService> TransactionParticipantService_;


    // Coordinator implementation.

    TFuture<TSharedRefArray> CoordinatorCommitTransaction(
        const TTransactionId& transactionId,
        const std::vector<TCellId>& participantCellIds,
        bool force2PC,
        bool inheritCommitTimestamp,
        const TMutationId& mutationId)
    {
        YCHECK(!HasMutationContext());

        auto* commit = FindCommit(transactionId);
        if (commit) {
            // NB: Even Response Keeper cannot protect us from this.
            return commit->GetAsyncResponseMessage();
        }

        commit = CreateTransientCommit(
            transactionId,
            mutationId,
            participantCellIds,
            force2PC || !participantCellIds.empty(),
            inheritCommitTimestamp);

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
        YCHECK(!commit->GetPersistent());

        const auto& transactionId = commit->GetTransactionId();

        try {
            // Any exception thrown here is replied to the client.
            auto prepareTimestamp = TimestampProvider_->GetLatestTimestamp();
            TransactionManager_->PrepareTransactionCommit(transactionId, false, prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing simple transaction commit (TransactionId: %v)",
                transactionId);
            SetCommitFailed(commit, ex);
            RemoveTransientCommit(commit);
            // Best effort, fire-and-forget.
            AbortTransaction(transactionId, true);
            return;
        }

        GenerateCommitTimestamps(commit);
    }

    void CommitDistributedTransaction(TCommit* commit)
    {
        YCHECK(!commit->GetPersistent());

        NHiveServer::NProto::TReqCoordinatorCommitDistributedTransactionPhaseOne request;
        ToProto(request.mutable_transaction_id(), commit->GetTransactionId());
        ToProto(request.mutable_mutation_id(), commit->GetMutationId());
        ToProto(request.mutable_participant_cell_ids(), commit->ParticipantCellIds());
        request.set_inherit_commit_timestamp(commit->GetInheritCommitTimestamp());
        request.set_prepare_timestamp(TimestampProvider_->GetLatestTimestamp());
        CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger);
    }

    TFuture<TSharedRefArray> CoordinatorAbortTransaction(
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        bool force)
    {
        YCHECK(!HasMutationContext());

        auto* abort = FindAbort(transactionId);
        if (abort) {
            // NB: Even Response Keeper cannot protect us from this.
            return abort->GetAsyncResponseMessage();
        }

        abort = CreateAbort(transactionId, mutationId);

        // Abort instance may die below.
        auto asyncResponseMessage = abort->GetAsyncResponseMessage();

        try {
            // Any exception thrown here is caught below..
            TransactionManager_->PrepareTransactionAbort(transactionId, force);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing transaction abort (TransactionId: %v, Force: %v)",
                transactionId,
                force);
            SetAbortFailed(abort, ex);
            RemoveAbort(abort);
            return asyncResponseMessage;
        }

        NHiveServer::NProto::TReqCoordinatorAbortTransaction request;
        ToProto(request.mutable_transaction_id(), transactionId);
        ToProto(request.mutable_mutation_id(), mutationId);
        request.set_force(force);
        CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger);

        return asyncResponseMessage;
    }

    static TFuture<void> MessageToError(TFuture<TSharedRefArray> asyncMessage)
    {
        return asyncMessage.Apply(BIND([] (const TSharedRefArray& message) -> TFuture<void> {
            TResponseHeader header;
            YCHECK(ParseResponseHeader(message, &header));
            return header.has_error()
                ? MakeFuture<void>(FromProto<TError>(header.error()))
                : VoidFuture;
        }));
    }


    // Hydra handlers.

    void HydraCoordinatorCommitSimpleTransaction(NHiveServer::NProto::TReqCoordinatorCommitSimpleTransaction* request)
    {
        auto mutationId = FromProto<TMutationId>(request->mutation_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamps = FromProto<TTimestampMap>(request->commit_timestamps());

        auto* commit = FindCommit(transactionId);

        if (commit && commit->GetPersistentState() != ECommitState::Start) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Requested to commit simple transaction in wrong state; ignored (TransactionId: %v, State: %v)",
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
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            if (commit) {
                SetCommitFailed(commit, ex);
                RemoveTransientCommit(commit);
            }
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Error committing simple transaction (TransactionId: %v)",
                transactionId);
            return;
        }

        if (!commit) {
            // Commit could be missing (e.g. at followers or during recovery).
            // Let's recreate it since it's needed below in SetCommitSucceeded.
            commit = CreateTransientCommit(
                transactionId,
                mutationId,
                std::vector<TCellId>(),
                false,
                false);
            commit->CommitTimestamps() = commitTimestamps;
        }

        SetCommitSucceeded(commit);
        RemoveTransientCommit(commit);
    }

    void HydraCoordinatorCommitDistributedTransactionPhaseOne(NHiveServer::NProto::TReqCoordinatorCommitDistributedTransactionPhaseOne* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mutationId = FromProto<TMutationId>(request->mutation_id());
        auto participantCellIds = FromProto<std::vector<TCellId>>(request->participant_cell_ids());
        auto inheritCommitTimestamp = request->inherit_commit_timestamp();
        auto prepareTimestamp = request->has_prepare_timestamp() ? request->prepare_timestamp() : MinTimestamp;

        // Ensure commit existence (possibly moving it from transient to persistent).
        auto* commit = GetOrCreatePersistentCommit(
            transactionId,
            mutationId,
            participantCellIds,
            true,
            inheritCommitTimestamp);

        if (commit && commit->GetPersistentState() != ECommitState::Start) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Requested to commit distributed transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Distributed commit phase one started (TransactionId: %v, ParticipantCellIds: %v, PrepareTimestamp: %llx)",
            transactionId,
            participantCellIds,
            prepareTimestamp);

        // Prepare at coordinator.
        try {
            // Any exception thrown here is caught below.
            TransactionManager_->PrepareTransactionCommit(transactionId, true, prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Coordinator failure; will abort (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Prepare);
            SetCommitFailed(commit, ex);
            RemovePersistentCommit(commit);
            try {
                TransactionManager_->AbortTransaction(transactionId, true);
            } catch (const std::exception& ex) {
                LOG_DEBUG_UNLESS(IsRecovery(), ex, "Error aborting transaction at coordinator; ignored (TransactionId: %v)",
                    transactionId);
            }
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Coordinator success (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Prepare);

        ChangeCommitPersistentState(commit, ECommitState::Prepare);
        ChangeCommitTransientState(commit, ECommitState::Prepare);
    }

    void HydraCoordinatorCommitDistributedTransactionPhaseTwo(NHiveServer::NProto::TReqCoordinatorCommitDistributedTransactionPhaseTwo* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamps = FromProto<TTimestampMap>(request->commit_timestamps());

        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to execute phase two commit for a non-existing transaction; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Distributed commit phase two started "
            "(TransactionId: %v, ParticipantCellIds: %v, CommitTimestamps: %v)",
            transactionId,
            commit->ParticipantCellIds(),
            commitTimestamps);

        YCHECK(commit->GetDistributed());
        YCHECK(commit->GetPersistent());

        if (commit->GetPersistentState() != ECommitState::Prepare) {
            LOG_ERROR_UNLESS(IsRecovery(),
                "Requested to execute phase two commit for transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        commit->CommitTimestamps() = commitTimestamps;
        ChangeCommitPersistentState(commit, ECommitState::Commit);
        ChangeCommitTransientState(commit, ECommitState::Commit);

        SetCommitSucceeded(commit);

        try {
            // Any exception thrown here is caught below.
            auto commitTimestamp = commitTimestamps.GetTimestamp(CellTagFromId(SelfCellId_));
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Unexpected error: coordinator failure; ignored (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Commit);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Coordinator success (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Commit);
    }

    void HydraCoordinatorAbortDistributedTransactionPhaseTwo(NHiveServer::NProto::TReqCoordinatorAbortDistributedTransactionPhaseTwo* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto error = FromProto<TError>(request->error());

        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            LOG_ERROR_UNLESS(IsRecovery(),
                "Requested to execute phase two abort for a non-existing transaction; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        YCHECK(commit->GetDistributed());
        YCHECK(commit->GetPersistent());

        if (commit->GetPersistentState() != ECommitState::Prepare) {
            LOG_ERROR_UNLESS(IsRecovery(),
                "Requested to execute phase two abort for transaction in wrong state; ignored (TransactionId: %v, State: %v)",
                transactionId,
                commit->GetPersistentState());
            return;
        }

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->AbortTransaction(transactionId, true);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error aborting transaction at coordinator; ignored (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Abort);
        }

        SetCommitFailed(commit, error);
        ChangeCommitPersistentState(commit, ECommitState::Abort);
        ChangeCommitTransientState(commit, ECommitState::Abort);

        LOG_DEBUG_UNLESS(IsRecovery(), "Coordinator aborted (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Abort);
    }

    void HydraCoordinatorAbortTransaction(NHiveServer::NProto::TReqCoordinatorAbortTransaction* request)
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
            TransactionManager_->AbortTransaction(transactionId, force);
        } catch (const std::exception& ex) {
            SetAbortFailed(abort, ex);
            RemoveAbort(abort);
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Error aborting transaction; ignored (TransactionId: %v)",
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

    void HydraCoordinatorFinishDistributedTransaction(NHiveServer::NProto::TReqCoordinatorFinishDistributedTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto* commit = FindPersistentCommit(transactionId);
        if (!commit) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Requested to finish a non-existing transaction commit; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        RemovePersistentCommit(commit);

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed transaction commit finished (TransactionId: %v)",
            transactionId);
    }

    void HydraParticipantPrepareTransaction(NHiveServer::NProto::TReqParticipantPrepareTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prepareTimestamp = request->prepare_timestamp();

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->PrepareTransactionCommit(transactionId, true, prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Participant failure (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Prepare);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant success (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Prepare);
    }

    void HydraParticipantCommitTransaction(NHiveServer::NProto::TReqParticipantCommitTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamp = request->commit_timestamp();

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Participant failure (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Commit);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant success (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Commit);
    }

    void HydraParticipantAbortTransaction(NHiveServer::NProto::TReqParticipantAbortTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->AbortTransaction(transactionId, true);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Participant failure (TransactionId: %v, State: %v)",
                transactionId,
                ECommitState::Abort);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant success (TransactionId: %v, State: %v)",
            transactionId,
            ECommitState::Abort);
    }


    TCommit* FindTransientCommit(const TTransactionId& transactionId)
    {
        return TransientCommitMap_.Find(transactionId);
    }

    TCommit* FindPersistentCommit(const TTransactionId& transactionId)
    {
        return PersistentCommitMap_.Find(transactionId);
    }

    TCommit* FindCommit(const TTransactionId& transactionId)
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
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        const std::vector<TCellId>& participantCellIds,
        bool distributed,
        bool inheritCommitTimestamp)
    {
        auto commitHolder = std::make_unique<TCommit>(
            transactionId,
            mutationId,
            participantCellIds,
            distributed,
            inheritCommitTimestamp);
        return TransientCommitMap_.Insert(transactionId, std::move(commitHolder));
    }

    TCommit* GetOrCreatePersistentCommit(
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        const std::vector<TCellId>& participantCellIds,
        bool distributed,
        bool inheritCommitTimstamp)
    {
        auto* commit = FindCommit(transactionId);
        std::unique_ptr<TCommit> commitHolder;
        if (commit) {
            YCHECK(!commit->GetPersistent());
            commitHolder = TransientCommitMap_.Release(transactionId);
        } else {
            commitHolder = std::make_unique<TCommit>(
                transactionId,
                mutationId,
                participantCellIds,
                distributed,
                inheritCommitTimstamp);
        }
        commitHolder->SetPersistent(true);
        return PersistentCommitMap_.Insert(transactionId, std::move(commitHolder));
    }


    void RemoveTransientCommit(TCommit* commit)
    {
        YCHECK(!commit->GetPersistent());
        TransientCommitMap_.Remove(commit->GetTransactionId());
    }

    void RemovePersistentCommit(TCommit* commit)
    {
        YCHECK(commit->GetPersistent());
        PersistentCommitMap_.Remove(commit->GetTransactionId());
    }


    void SetCommitFailed(TCommit* commit, const TError& error)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), error, "Transaction commit failed (TransactionId: %v)",
            commit->GetTransactionId());

        auto responseMessage = CreateErrorResponseMessage(error);
        SetCommitResponse(commit, responseMessage);
    }

    void SetCommitSucceeded(TCommit* commit)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit succeeded (TransactionId: %v, CommitTimestamps: %v)",
            commit->GetTransactionId(),
            commit->CommitTimestamps());

        NHiveClient::NProto::NTransactionSupervisor::TRspCommitTransaction response;
        ToProto(response.mutable_commit_timestamps(), commit->CommitTimestamps());

        auto responseMessage = CreateResponseMessage(response);
        SetCommitResponse(commit, std::move(responseMessage));
    }

    void SetCommitResponse(TCommit* commit, TSharedRefArray responseMessage)
    {
        const auto& mutationId = commit->GetMutationId();
        if (mutationId) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage);
        }

        commit->SetResponseMessage(std::move(responseMessage));
    }


    TAbort* FindAbort(const TTransactionId& transactionId)
    {
        auto it = TransientAbortMap_.find(transactionId);
        return it == TransientAbortMap_.end() ? nullptr : &it->second;
    }

    TAbort* CreateAbort(const TTransactionId& transactionId, const TMutationId& mutationId)
    {
        auto pair = TransientAbortMap_.emplace(transactionId, TAbort(transactionId, mutationId));
        YCHECK(pair.second);
        return &pair.first->second;
    }

    void SetAbortFailed(TAbort* abort, const TError& error)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), error, "Transaction abort failed (TransactionId: %v)",
            abort->GetTransactionId());

        auto responseMessage = CreateErrorResponseMessage(error);
        SetAbortResponse(abort, std::move(responseMessage));
    }

    void SetAbortSucceeded(TAbort* abort)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction abort succeeded (TransactionId: %v)",
            abort->GetTransactionId());

        NHiveClient::NProto::NTransactionSupervisor::TRspAbortTransaction response;

        auto responseMessage = CreateResponseMessage(response);
        SetAbortResponse(abort, std::move(responseMessage));
    }

    void SetAbortResponse(TAbort* abort, TSharedRefArray responseMessage)
    {
        const auto& mutationId = abort->GetMutationId();
        if (mutationId) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage);
        }

        abort->SetResponseMessage(std::move(responseMessage));
    }

    void RemoveAbort(TAbort* abort)
    {
        YCHECK(TransientAbortMap_.erase(abort->GetTransactionId()) == 1);
    }


    void GenerateCommitTimestamps(TCommit* commit)
    {
        const auto& transactionId = commit->GetTransactionId();

        TFuture<TTimestamp> asyncCoordinatorTimestamp;
        std::vector<TFuture<std::pair<TCellTag, TTimestamp>>> asyncTimestamps;
        yhash_set<TCellTag> timestampProviderCellTags;
        auto generateFor = [&] (const TCellId& cellId) {
            try {
                auto cellTag = CellTagFromId(cellId);
                if (!timestampProviderCellTags.insert(cellTag).second) {
                    return;
                }

                auto participant = GetParticipant(cellId);
                auto timestampProvider = participant->GetTimestampProviderOrThrow();

                TFuture<TTimestamp> asyncTimestamp;
                if (commit->GetInheritCommitTimestamp() && cellId != SelfCellId_) {
                    LOG_DEBUG("Inheriting commit timestamp (TransactionId: %v, ParticipantCellId: %v)",
                        transactionId,
                        cellId);
                    YCHECK(asyncCoordinatorTimestamp);
                    asyncTimestamp = asyncCoordinatorTimestamp;
                } else {
                    LOG_DEBUG("Generating commit timestamp (TransactionId: %v, ParticipantCellId: %v)",
                        transactionId,
                        cellId);
                    asyncTimestamp = timestampProvider->GenerateTimestamps(1);
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
        for (const auto& cellId : commit->ParticipantCellIds()) {
            generateFor(cellId);
        }

        Combine(asyncTimestamps)
            .Subscribe(BIND(&TImpl::OnCommitTimestampsGenerated, MakeStrong(this), transactionId)
                .Via(EpochAutomatonInvoker_));
    }

    void OnCommitTimestampsGenerated(
        const TTransactionId& transactionId,
        const TErrorOr<std::vector<std::pair<TCellTag, TTimestamp>>>& timestampsOrError)
    {
        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_DEBUG("Commit timestamp generated for a non-existing transaction commit; ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        if (!timestampsOrError.IsOK()) {
            // If this is a distributed transaction then it's already prepared at coordinator and
            // at all participants. We _must_ forcefully abort it.
            LOG_DEBUG(timestampsOrError, "Error generating commit timestamps (TransactionId: %v)",
                transactionId);
            AbortTransaction(transactionId, true);
            return;
        }

        const auto& result = timestampsOrError.Value();

        TTimestampMap commitTimestamps;
        commitTimestamps.Timestamps.insert(commitTimestamps.Timestamps.end(), result.begin(), result.end());

        LOG_DEBUG("Commit timestamps generated (TransactionId: %v, CommitTimestamps: %v)",
            transactionId,
            commitTimestamps);

        if (commit->GetDistributed()) {
            NHiveServer::NProto::TReqCoordinatorCommitDistributedTransactionPhaseTwo request;
            ToProto(request.mutable_transaction_id(), transactionId);
            ToProto(request.mutable_commit_timestamps(), commitTimestamps);
            CreateMutation(HydraManager_, request)
                ->CommitAndLog(Logger);
        } else {
            NHiveServer::NProto::TReqCoordinatorCommitSimpleTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            ToProto(request.mutable_mutation_id(), commit->GetMutationId());
            ToProto(request.mutable_commit_timestamps(), commitTimestamps);
            CreateMutation(HydraManager_, request)
                ->CommitAndLog(Logger);
        }
    }


    TWrappedParticipantPtr GetParticipant(const TCellId& cellId)
    {
        auto it = WeakParticipantMap_.find(cellId);
        if (it != WeakParticipantMap_.end()) {
            auto participant = it->second.Lock();
            if (participant) {
                return participant;
            }
            WeakParticipantMap_.erase(it);
        }

        auto wrappedParticipant = New<TWrappedParticipant>(
            cellId,
            Config_,
            TimestampProvider_,
            ParticipantProviders_,
            Logger);

        YCHECK(StrongParticipantMap_.emplace(cellId, wrappedParticipant).second);
        YCHECK(WeakParticipantMap_.emplace(cellId, wrappedParticipant).second);

        LOG_DEBUG("Participant cell registered (ParticipantCellId: %v)",
            cellId);

        return wrappedParticipant;
    }

    void OnParticipantCleanup()
    {
        for (auto it = StrongParticipantMap_.begin(); it != StrongParticipantMap_.end(); ) {
            auto jt = it++;
            if (!jt->second->IsValid()) {
                LOG_DEBUG("Participant cell unregistered (ParticipantCellId: %v)",
                    jt->first);
                StrongParticipantMap_.erase(jt);
            }
        }

        for (auto it = WeakParticipantMap_.begin(); it != WeakParticipantMap_.end(); ) {
            auto jt = it++;
            if (jt->second.IsExpired()) {
                WeakParticipantMap_.erase(jt);
            }
        }
    }


    void ChangeCommitTransientState(TCommit* commit, ECommitState state, const TError& error = TError())
    {
        if (!IsLeader()) {
            return;
        }

        LOG_DEBUG("Commit transient state changed (TransactionId: %v, State: %v -> %v)",
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
                NHiveServer::NProto::TReqCoordinatorAbortDistributedTransactionPhaseTwo request;
                ToProto(request.mutable_transaction_id(), commit->GetTransactionId());
                ToProto(request.mutable_error(), error);
                CreateMutation(HydraManager_, request)
                    ->CommitAndLog(Logger);
                break;
            }

            case ECommitState::Finishing: {
                NHiveServer::NProto::TReqCoordinatorFinishDistributedTransaction request;
                ToProto(request.mutable_transaction_id(), commit->GetTransactionId());
                CreateMutation(HydraManager_, request)
                    ->CommitAndLog(Logger);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    void ChangeCommitPersistentState(TCommit* commit, ECommitState state)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "Commit persistent state changed (TransactionId: %v, State: %v -> %v)",
            commit->GetTransactionId(),
            commit->GetPersistentState(),
            state);
        commit->SetPersistentState(state);
    }

    void SendParticipantRequests(TCommit* commit)
    {
        YCHECK(commit->RespondedCellIds().empty());
        for (const auto& cellId : commit->ParticipantCellIds()) {
            SendParticipantRequest(commit, cellId);
        }
        CheckAllParticipantsResponded(commit);
    }

    void SendParticipantRequest(TCommit* commit, const TCellId& cellId)
    {
        auto participant = GetParticipant(cellId);

        TFuture<void> response;
        auto state = commit->GetTransientState();
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
                Y_UNREACHABLE();
        }
        response.Subscribe(
            BIND(&TImpl::OnParticipantResponse, MakeWeak(this), commit->GetTransactionId(), state, participant)
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
            LOG_DEBUG("Transaction is missing at participant; still consider this a success "
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
        if (error.IsOK()) {
            return true;
        }

        if (error.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
            return true;
        }

        return false;
    }

    void OnParticipantResponse(
        const TTransactionId& transactionId,
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
            LOG_DEBUG("Received participant response for a non-existing commit; ignored (TransactionId: %v, ParticipantCellId: %v)",
                transactionId,
                participantCellId);
            return;
        }

        if (state != commit->GetTransientState()) {
            LOG_DEBUG("Received participant response for a commit in wrong state; ignored (TransactionId: %v, "
                "ParticipantCellId: %v, ExpectedState: %v, ActualState: %v)",
                transactionId,
                participantCellId,
                state,
                commit->GetTransientState());
            return;
        }

        if (IsParticipantResponseSuccessful(commit, participant, error)) {
            LOG_DEBUG("Coordinator observes participant success "
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
                    LOG_DEBUG(error, "Coordinator observes participant failure; will abort "
                        "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                        commit->GetTransactionId(),
                        participantCellId,
                        state);
                    auto wrappedError = TError("Participant %v has failed to prepare", participantCellId)
                        << error;
                    ChangeCommitTransientState(commit, ECommitState::Aborting, wrappedError);
                    break;
                }

                case ECommitState::Commit:
                case ECommitState::Abort:
                    LOG_DEBUG(error, "Coordinator observes participant failure; will retry "
                        "(TransactionId: %v, ParticipantCellId: %v, State: %v)",
                        commit->GetTransactionId(),
                        participantCellId,
                        state);
                    SendParticipantRequest(commit, participantCellId);
                    break;

                default:
                    LOG_DEBUG(error, "Coordinator observes participant failure; ignored "
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
                Y_UNREACHABLE();
        }
    }


    virtual bool ValidateSnapshotVersion(int version) override
    {
        return
            version == 1 ||
            version == 2 ||
            version == 3;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 3;
    }


    virtual void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderActive();

        ParticipantCleanupExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND(&TImpl::OnParticipantCleanup, MakeWeak(this)),
            ParticipantCleanupPeriod);
        ParticipantCleanupExecutor_->Stop();

        YCHECK(TransientCommitMap_.GetSize() == 0);
        for (const auto& pair : PersistentCommitMap_) {
            auto* commit = pair.second;
            ChangeCommitTransientState(commit, commit->GetPersistentState());
        }
    }

    virtual void OnStopLeading() override
    {
        TCompositeAutomatonPart::OnStopLeading();

        if (ParticipantCleanupExecutor_) {
            ParticipantCleanupExecutor_->Stop();
        }
        ParticipantCleanupExecutor_.Reset();

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");

        for (const auto& pair : TransientCommitMap_) {
            auto* commit = pair.second;
            SetCommitFailed(commit, error);
        }
        TransientCommitMap_.Clear();

        for (auto& pair : TransientAbortMap_) {
            auto* abort = &pair.second;
            SetAbortFailed(abort, error);
        }
        TransientAbortMap_.clear();

        TransientCommitMap_.Clear();
        StrongParticipantMap_.clear();
        WeakParticipantMap_.clear();
    }


    virtual void Clear() override
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
    }

    void LoadKeys(TLoadContext& context)
    {
        PersistentCommitMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        PersistentCommitMap_.LoadValues(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionSupervisor::TTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IInvokerPtr trackerInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    TResponseKeeperPtr responseKeeper,
    ITransactionManagerPtr transactionManager,
    const TCellId& selfCellId,
    ITimestampProviderPtr timestampProvider,
    const std::vector<ITransactionParticipantProviderPtr>& participantProviders)
    : Impl_(New<TImpl>(
        config,
        automatonInvoker,
        trackerInvoker,
        hydraManager,
        automaton,
        responseKeeper,
        transactionManager,
        selfCellId,
        timestampProvider,
        participantProviders))
{ }

TTransactionSupervisor::~TTransactionSupervisor() = default;

std::vector<NRpc::IServicePtr> TTransactionSupervisor::GetRpcServices()
{
    return Impl_->GetRpcServices();
}

TFuture<void> TTransactionSupervisor::CommitTransaction(
    const TTransactionId& transactionId,
    const std::vector<TCellId>& participantCellIds)
{
    return Impl_->CommitTransaction(
        transactionId,
        participantCellIds);
}

TFuture<void> TTransactionSupervisor::AbortTransaction(
    const TTransactionId& transactionId,
    bool force)
{
    return Impl_->AbortTransaction(
        transactionId,
        force);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
