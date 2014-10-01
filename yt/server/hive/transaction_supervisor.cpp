#include "stdafx.h"
#include "transaction_supervisor.h"
#include "config.h"
#include "transaction_manager.h"
#include "hive_manager.h"
#include "commit.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>
#include <core/rpc/message.h>
#include <core/rpc/response_keeper.h>
#include <core/rpc/rpc.pb.h>

#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/hive/transaction_supervisor_service_proxy.h>

#include <ytlib/transaction_client/timestamp_provider.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/entity_map.h>
#include <server/hydra/hydra_service.h>
#include <server/hydra/rpc_helpers.h>

#include <server/election/election_manager.h>

#include <server/hive/transaction_supervisor.pb.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra;
using namespace NHive::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor::TImpl
    : public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IResponseKeeperPtr responseKeeper,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager,
        ITimestampProviderPtr timestampProvider)
        : THydraServiceBase(
            hydraManager,
            automatonInvoker,
            TServiceId(TTransactionSupervisorServiceProxy::GetServiceName(), hiveManager->GetSelfCellGuid()),
            HiveLogger)
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , Config_(config)
        , ResponseKeeper_(responseKeeper)
        , HiveManager_(hiveManager)
        , TransactionManager_(transactionManager)
        , TimestampProvider_(timestampProvider)
    {
        YCHECK(Config_);
        YCHECK(ResponseKeeper_);
        YCHECK(HiveManager_);
        YCHECK(TransactionManager_);
        YCHECK(TimestampProvider_);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartDistributedCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraFinalizeDistributedCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPrepareTransactionCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnTransactionCommitPrepared, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitPreparedTransaction, Unretained(this)));

        RegisterLoader(
            "TransactionSupervisor.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionSupervisor.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TransactionSupervisor.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TransactionSupervisor.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    IServicePtr GetRpcService()
    {
        return this;
    }

    TAsyncError CommitTransaction(
        const TTransactionId& transactionId,
        const std::vector<TCellGuid>& participantCellGuids)
    {
        return MessageToError(DoCommitTransaction(
            transactionId,
            participantCellGuids,
            NullMutationId));
    }

    TAsyncError AbortTransaction(const TTransactionId& transactionId)
    {
        return MessageToError(DoAbortTransaction(
            transactionId,
            NullMutationId));
    }

private:
    TTransactionSupervisorConfigPtr Config_;
    IResponseKeeperPtr ResponseKeeper_;
    THiveManagerPtr HiveManager_;
    ITransactionManagerPtr TransactionManager_;
    ITimestampProviderPtr TimestampProvider_;

    TEntityMap<TTransactionId, TCommit> TransientCommitMap_;
    TEntityMap<TTransactionId, TCommit> PersistentCommitMap_;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request->participant_cell_guids());
        auto mutationId = GetMutationId(context);

        context->SetRequestInfo("TransactionId: %v, ParticipantCellGuids: [%v]",
            transactionId,
            JoinToString(participantCellGuids));

        auto asyncResponseMessage = DoCommitTransaction(transactionId, participantCellGuids, mutationId); 
        context->Reply(asyncResponseMessage);
    }


    DECLARE_RPC_SERVICE_METHOD(NProto, AbortTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mutationId = GetMutationId(context);

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto asyncResponseMessage = DoAbortTransaction(transactionId, mutationId);
        context->Reply(asyncResponseMessage);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingTransaction)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        // Any exception thrown here is replied to the client.
        TransactionManager_->PingTransaction(transactionId, *request);
        context->Reply();
    }


    // Facade implementation.

    TFuture<TSharedRefArray> DoCommitTransaction(
        const TTransactionId& transactionId,
        const std::vector<TCellGuid>& participantCellGuids,
        const TMutationId& mutationId)
    {
        try {
            ValidateActiveLeader();

            if (mutationId != NullMutationId) {
                auto asyncResponseMessage = ResponseKeeper_->TryBeginRequest(mutationId);
                if (asyncResponseMessage) {
                    return asyncResponseMessage;
                }
            }

            auto* commit = FindCommit(transactionId);
            if (commit) {
                THROW_ERROR_EXCEPTION("Transaction %v is already being committed",
                    transactionId);
            }

            commit = new TCommit(
                transactionId,
                mutationId,
                participantCellGuids);
            TransientCommitMap_.Insert(transactionId, commit);

            auto prepareTimestamp = TimestampProvider_->GetLatestTimestamp();

            if (participantCellGuids.empty()) {
                // Simple commit.
                try {
                    // Any exception thrown here is replied to the client.
                    TransactionManager_->PrepareTransactionCommit(
                        transactionId,
                        false,
                        prepareTimestamp);
                } catch (const std::exception& ex) {
                    auto error = TError(ex);
                    LOG_DEBUG(error, "Simple commit has failed to prepare (TransactionId: %v)",
                        transactionId);
                    SetCommitFailed(commit, error);
                    return commit->GetResult();
                }

                LOG_DEBUG_UNLESS(IsRecovery(), "Simple commit prepared (TransactionId: %v, PrepareTimestamp: %v)",
                    transactionId,
                    prepareTimestamp);

                RunCommit(commit);
            } else {
                // Distributed commit.
                TReqStartDistributedCommit hydraRequest;
                ToProto(hydraRequest.mutable_transaction_id(), transactionId);
                ToProto(hydraRequest.mutable_mutation_id(), mutationId);
                ToProto(hydraRequest.mutable_participant_cell_guids(), participantCellGuids);
                hydraRequest.set_prepare_timestamp(prepareTimestamp);
                CreateMutation(HydraManager, hydraRequest)
                    ->SetAction(BIND(&TImpl::HydraStartDistributedCommit, MakeStrong(this), hydraRequest))
                    ->Commit();
            }

            return commit->GetResult();
        } catch (const std::exception& ex) {
            auto responseMessage = CreateErrorResponseMessage(ex);
            return MakeFuture(responseMessage);
        }
    }

    TFuture<TSharedRefArray> DoAbortTransaction(
        const TTransactionId& transactionId,
        const TMutationId& mutationId)
    {
        try {
            ValidateActiveLeader();

            if (mutationId != NullMutationId) {
                auto asyncResponseMessage = ResponseKeeper_->TryBeginRequest(mutationId);
                if (asyncResponseMessage) {
                    return asyncResponseMessage;
                }
            }

            // Any exception thrown here is caught below.
            TransactionManager_->PrepareTransactionAbort(transactionId);

            TReqHydraAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            ToProto(hydraRequest.mutable_mutation_id(), mutationId);

            return CreateMutation(HydraManager, hydraRequest)
                ->Commit().Apply(BIND([] (TErrorOr<TMutationResponse> result) -> TSharedRefArray {
                    if (result.IsOK()) {
                        TRspAbortTransaction response;
                        return CreateResponseMessage(response);
                    } else {
                        return CreateErrorResponseMessage(result);
                    }
                }));
        } catch (const std::exception& ex) {
            auto responseMessage = CreateErrorResponseMessage(ex);
            return MakeFuture(responseMessage);
        }
    }

    static TAsyncError MessageToError(TFuture<TSharedRefArray> asyncMessage)
    {
        return asyncMessage.Apply(BIND([] (TSharedRefArray message) -> TError {
            TResponseHeader header;
            YCHECK(ParseResponseHeader(message, &header));
            return FromProto<TError>(header.error());
        }));
    }

    // Hydra handlers.

    void HydraAbortTransaction(const TReqHydraAbortTransaction& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto force = request.force();

        auto* commit = FindCommit(transactionId);
        DoAbortTransaction(transactionId, commit, force);

        if (mutationId != NullMutationId) {
            TRspAbortTransaction response;
            auto responseMessage = CreateResponseMessage(response);
            ResponseKeeper_->EndRequest(mutationId, std::move(responseMessage));
        }
    }

    void HydraStartDistributedCommit(const TReqStartDistributedCommit& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request.participant_cell_guids());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());

        auto* commit = TransientCommitMap_.Find(transactionId);
        if (commit) {
            TransientCommitMap_.Release(transactionId).release();
        } else {
            commit = new TCommit(
                transactionId,
                mutationId,
                participantCellGuids);
        }
        PersistentCommitMap_.Insert(transactionId, commit);

        const auto& coordinatorCellGuid = HiveManager_->GetSelfCellGuid();

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Distributed commit first phase started "
            "(TransactionId: %v, ParticipantCellGuids: [%v], CoordinatorCellGuid: %v)",
            transactionId,
            JoinToString(participantCellGuids),
            coordinatorCellGuid);

        // Prepare at coordinator.
        try {
            // Any exception thrown here is caught below.
            DoPrepareDistributed(
                transactionId,
                prepareTimestamp,
                coordinatorCellGuid,
                true);
        } catch (const std::exception& ex) {
            SetCommitFailed(commit, ex);
            return;
        }

        // Prepare at participants.
        {
            TReqPrepareTransactionCommit hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_prepare_timestamp(prepareTimestamp);
            ToProto(hydraRequest.mutable_coordinator_cell_guid(), coordinatorCellGuid);

            PostToParticipants(commit, hydraRequest);
        }
    }

    void HydraPrepareTransactionCommit(const TReqPrepareTransactionCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
        auto coordinatorCellGuid = FromProto<TCellGuid>(request.coordinator_cell_guid());

        TError error; // initially OK
        try {
            // Any exception thrown here is replied to the coordinator.
            DoPrepareDistributed(
                transactionId,
                prepareTimestamp,
                coordinatorCellGuid,
                false);
        } catch (const std::exception& ex) {
            error = ex;
        }

        {
            TReqOnTransactionCommitPrepared hydraResponse;
            ToProto(hydraResponse.mutable_transaction_id(), transactionId);
            ToProto(hydraResponse.mutable_participant_cell_guid(), HiveManager_->GetSelfCellGuid());
            ToProto(hydraResponse.mutable_error(), error);

            PostToCoordinator(coordinatorCellGuid, hydraResponse);
        }
    }

    void HydraOnTransactionCommitPrepared(const TReqOnTransactionCommitPrepared& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuid = FromProto<TCellGuid>(request.participant_cell_guid());

        auto* commit = PersistentCommitMap_.Find(transactionId);
        if (!commit) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Invalid or expired transaction has prepared, ignoring (TransactionId: %v)",
                transactionId);
            return;
        }

        auto error = FromProto<TError>(request.error());
        if (!error.IsOK()) {
            LOG_DEBUG_UNLESS(IsRecovery(), error, "Participant has failed to prepare (TransactionId: %v, ParticipantCellGuid: %v)",
                transactionId,
                participantCellGuid);
            SetCommitFailed(commit, error);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant has prepared (TransactionId: %v, ParticipantCellGuid: %v)",
            transactionId,
            participantCellGuid);

        YCHECK(commit->PreparedParticipantCellGuids().insert(participantCellGuid).second);

        if (IsLeader()) {
            CheckForSecondPhaseStart(commit);
        }
    }

    void HydraCommitPreparedTransaction(const TReqCommitPreparedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());
        bool isDistributed = request.is_distributed();

        DoCommitPrepared(
            transactionId,
            commitTimestamp,
            isDistributed,
            false);

        if (!isDistributed) {
            // Commit could be missing (e.g. at followers).
            auto* commit = FindCommit(transactionId);
            if (commit) {
                SetCommitCompleted(commit, commitTimestamp);
            }
        }
    }

    void HydraFinalizeDistributedCommit(const TReqFinalizeDistributedCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to finalize an invalid or expired transaction, ignoring (TransactionId: %v)",
                transactionId);
            return;
        }

        YCHECK(commit->IsDistributed());

        // Commit at coordinator.
        DoCommitPrepared(
            transactionId,
            commitTimestamp,
            true,
            true);

        // Commit at participants.
        {
            TReqCommitPreparedTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(commitTimestamp);
            hydraRequest.set_is_distributed(true);

            PostToParticipants(commit, hydraRequest);
        }

        SetCommitCompleted(commit, commitTimestamp);
    }


    TCommit* FindCommit(const TTransactionId& transactionId)
    {
        if (auto* commit = TransientCommitMap_.Find(transactionId)) {
            return commit;
        }
        if (auto* commit = PersistentCommitMap_.Find(transactionId)) {
            return commit;
        }
        return nullptr;
    }

    void SetCommitFailed(TCommit* commit, const TError& error)
    {
        auto responseMessage = CreateErrorResponseMessage(error);
        SetCommitResult(commit, responseMessage);

        const auto& transactionId = commit->GetTransactionId();

        if (HydraManager->IsMutating()) {
            DoAbortTransaction(transactionId, commit, true);
        } else {
            TReqHydraAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_force(true);

            CreateMutation(HydraManager, hydraRequest)
                ->Commit();
        }
    }

    void DoAbortTransaction(const TTransactionId& transactionId, TCommit* commit, bool force)
    {
        try {
            // All exceptions thrown here are caught below and ignored.
            TransactionManager_->AbortTransaction(transactionId, force);

            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v)",
                transactionId);

            if (commit) {
                TReqHydraAbortTransaction hydraRequest;
                ToProto(hydraRequest.mutable_transaction_id(), transactionId);
                hydraRequest.set_force(force);

                PostToParticipants(commit, hydraRequest);

                RemoveCommit(commit);
            }
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Error aborting transaction, ignoring (TransactionId: %v)",
                transactionId);
        }
    }


    void SetCommitCompleted(TCommit* commit, TTimestamp commitTimestamp)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "%v transaction commit completed (TransactionId: %v, CommitTimestamp: %v)",
            commit->IsDistributed() ? "Distributed" : "Simple",
            commit->GetTransactionId(),
            commitTimestamp);

        TRspCommitTransaction response;
        response.set_commit_timestamp(commitTimestamp);

        auto responseMessage = CreateResponseMessage(response);
        SetCommitResult(commit, responseMessage);

        RemoveCommit(commit);
    }

    void SetCommitResult(TCommit* commit, TSharedRefArray responseMessage)
    {
        const auto& mutationId = commit->GetMutationId();
        if (mutationId != NullMutationId && HydraManager->IsMutating()) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage);
        }

        commit->SetResult(std::move(responseMessage));
    }

    void RemoveCommit(TCommit* commit)
    {
        if (commit->IsDistributed()) {
            PersistentCommitMap_.Remove(commit->GetTransactionId());
        } else {
            TransientCommitMap_.Remove(commit->GetTransactionId());
        }
    }


    template <class TMessage>
    void PostToParticipants(TCommit* commit, const TMessage& message)
    {
        for (const auto& cellGuid : commit->ParticipantCellGuids()) {
            auto* mailbox = HiveManager_->GetOrCreateMailbox(cellGuid);
            HiveManager_->PostMessage(mailbox, message);
        }
    }

    template <class TMessage>
    void PostToCoordinator(const TCellGuid& coordinatorCellGuid, const TMessage& message)
    {
        auto* mailbox = HiveManager_->GetOrCreateMailbox(coordinatorCellGuid);
        HiveManager_->PostMessage(mailbox, message);
    }


    void RunCommit(TCommit* commit)
    {
        TimestampProvider_->GenerateTimestamps()
            .Subscribe(BIND(&TImpl::OnCommitTimestampGenerated, MakeStrong(this), commit->GetTransactionId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnCommitTimestampGenerated(
        const TTransactionId& transactionId,
        TErrorOr<TTimestamp> timestampOrError) 
    {
        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_DEBUG("Commit timestamp generated for an invalid or expired transaction, ignoring (TransactionId: %v)",
                transactionId);
            return;
        }

        if (!timestampOrError.IsOK()) {
            auto error = TError("Error generating commit timestamp")
                << timestampOrError;
            LOG_ERROR(error);
            SetCommitFailed(commit, error);
            return;
        }

        auto timestamp = timestampOrError.Value();

        if (commit->IsDistributed()) {
            TReqFinalizeDistributedCommit hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(timestamp);
            CreateMutation(HydraManager, hydraRequest)
                ->Commit();
        } else {
            TReqCommitPreparedTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(timestamp);
            hydraRequest.set_is_distributed(false);
            CreateMutation(HydraManager, hydraRequest)
                ->Commit();
        }
    }


    void DoPrepareDistributed(
        const TTransactionId& transactionId,
        TTimestamp prepareTimestamp,
        const TCellGuid& coordinatorCellGuid,
        bool isCoordinator)
    {
        // Any exception thrown here is propagated to the caller.
        try {
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                true,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Failed to prepare distributed commit (TransactionId: %v, CoordinatorCellGuid: %v, PrepareTimestamp: %v)",
                transactionId,
                coordinatorCellGuid,
                prepareTimestamp);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Distirbuted commit is prepared by %v (TransactionId: %v, CoordinatorCellGuid: %v, PrepareTimestamp: %v)",
            isCoordinator ? "coordinator" : "participant",
            transactionId,
            coordinatorCellGuid,
            prepareTimestamp);
    }

    void DoCommitPrepared(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp,
        bool isDistributed,
        bool isCoordinator)
    {
        try {
            // Any exception thrown here is caught below.
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error committing prepared transaction");
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "%v transaction committed %v(TransactionId: %v, CommitTimestamp: %v)",
            isDistributed ? "Distributed" : "Simple",
            isDistributed ? (isCoordinator ? "by coordinator " : "by participant ") : "",
            transactionId,
            commitTimestamp);
    }

    void DoAbortFailed(const TTransactionId& transactionId)
    {
        try {
            // All exceptions thrown here are caught below and ignored.
            TransactionManager_->AbortTransaction(transactionId, true);
            LOG_DEBUG_UNLESS(IsRecovery(), "Failed transaction aborted (TransactionId: %v)",
                transactionId);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Error aborting failed transaction, ignoring (TransactionId: %v)",
                transactionId);
        }
    }


    void CheckForSecondPhaseStart(TCommit* commit)
    {
        if (commit->ParticipantCellGuids().empty())
            // Not a distributed commit.
            return;
        
        if (commit->PreparedParticipantCellGuids().size() != commit->ParticipantCellGuids().size())
            // Some participants are not prepared yet.
            return;

        const auto& transactionId = commit->GetTransactionId();

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit second phase started (TransactionId: %v)",
            transactionId);

        RunCommit(commit);
    }

    
    virtual bool ValidateSnapshotVersion(int version) override
    {
        return version == 1;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 1;
    }


    virtual void OnLeaderActive() override
    {
        for (const auto& pair : PersistentCommitMap_) {
            auto* commit = pair.second;
            CheckForSecondPhaseStart(commit);
        }
    }

    virtual void OnStopLeading() override
    {
        TransientCommitMap_.Clear();
    }


    virtual void Clear() override
    {
        PersistentCommitMap_.Clear();
        TransientCommitMap_.Clear();
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
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IResponseKeeperPtr responseKeeper,
    THiveManagerPtr hiveManager,
    ITransactionManagerPtr transactionManager,
    ITimestampProviderPtr timestampProvider)
    : Impl_(New<TImpl>(
        config,
        automatonInvoker,
        hydraManager,
        automaton,
        responseKeeper,
        hiveManager,
        transactionManager,
        timestampProvider))
{ }

TTransactionSupervisor::~TTransactionSupervisor()
{ }

IServicePtr TTransactionSupervisor::GetRpcService()
{
    return Impl_->GetRpcService();
}

TAsyncError TTransactionSupervisor::CommitTransaction(
    const TTransactionId& transactionId,
    const std::vector<TCellGuid>& participantCellGuids)
{
    return Impl_->CommitTransaction(
        transactionId,
        participantCellGuids);
}

TAsyncError TTransactionSupervisor::AbortTransaction(const TTransactionId& transactionId)
{
    return Impl_->AbortTransaction(transactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
