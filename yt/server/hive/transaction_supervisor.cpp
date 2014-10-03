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

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitSimpleTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitDistributedTransactionPhaseOne, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPrepareTransactionCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnTransactionCommitPrepared, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitDistributedTransactionPhaseTwo, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitPreparedTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this)));

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

    TAsyncError AbortTransaction(
        const TTransactionId& transactionId,
        bool force)
    {
        return MessageToError(DoAbortTransaction(
            transactionId,
            NullMutationId,
            force));
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
        ValidateActiveLeader();

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
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        bool force = request->force();
        auto mutationId = GetMutationId(context);

        context->SetRequestInfo("TransactionId: %v, Force: %v",
            transactionId,
            force);

        auto asyncResponseMessage = DoAbortTransaction(transactionId, mutationId, force);
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
        YASSERT(!HydraManager->IsMutating());

        if (mutationId != NullMutationId) {
            auto asyncResponseMessage = ResponseKeeper_->TryBeginRequest(mutationId);
            if (asyncResponseMessage) {
                return asyncResponseMessage;
            }
        }

        auto* commit = FindCommit(transactionId);
        if (commit) {
            // NB: Even Response Keeper cannot protect us from this.
            return commit->GetAsyncResponseMessage();
        }

        commit = new TCommit(
            transactionId,
            mutationId,
            participantCellGuids);
        TransientCommitMap_.Insert(transactionId, commit);

        // Commit instance may die below.
        auto asyncResponseMessage = commit->GetAsyncResponseMessage();

        if (participantCellGuids.empty()) {
            DoCommitSimpleTransaction(commit);
        } else {
            DoCommitDistributedTransaction(commit);
        }

        return asyncResponseMessage;
    }

    void DoCommitSimpleTransaction(TCommit* commit)
    {
        auto prepareTimestamp = TimestampProvider_->GetLatestTimestamp();
        const auto& transactionId = commit->GetTransactionId();

        try {
            // Any exception thrown here is replied to the client.
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                false,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing simple transaction commit (TransactionId: %v)",
                transactionId);
            SetCommitFailed(commit, ex);
            RemoveCommit(commit);
            // Best effort, fire-and-forget.
            AbortTransaction(transactionId, false);
            return;
        }

        GenerateCommitTimestamp(commit);
    }

    void DoCommitDistributedTransaction(TCommit* commit)
    {
        auto prepareTimestamp = TimestampProvider_->GetLatestTimestamp();

        // Distributed commit.
        TReqCommitDistributedTransactionPhaseOne hydraRequest;
        ToProto(hydraRequest.mutable_transaction_id(), commit->GetTransactionId());
        ToProto(hydraRequest.mutable_mutation_id(), commit->GetMutationId());
        ToProto(hydraRequest.mutable_participant_cell_guids(), commit->ParticipantCellGuids());
        hydraRequest.set_prepare_timestamp(prepareTimestamp);
        CreateMutation(HydraManager, hydraRequest)
            ->Commit();
    }

    TFuture<TSharedRefArray> DoAbortTransaction(
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        bool force)
    {
        YASSERT(!HydraManager->IsMutating());

        if (mutationId != NullMutationId) {
            auto asyncResponseMessage = ResponseKeeper_->TryBeginRequest(mutationId);
            if (asyncResponseMessage) {
                return asyncResponseMessage;
            }
        }

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->PrepareTransactionAbort(transactionId, force);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing transaction abort (TransactionId: %v, Force: %v)",
                transactionId,
                force);
            auto responseMessage = CreateErrorResponseMessage(ex);
            if (mutationId != NullMutationId) {
                ResponseKeeper_->EndRequest(mutationId, responseMessage);
            }
            return MakeFuture(responseMessage);
        }

        TReqHydraAbortTransaction hydraRequest;
        ToProto(hydraRequest.mutable_transaction_id(), transactionId);
        ToProto(hydraRequest.mutable_mutation_id(), mutationId);
        hydraRequest.set_force(force);

        // If the mutation succeeds then Response Keeper gets notified in HydraAbortTransaction.
        // If it fails then the current epoch ends and Response Keeper gets cleaned up anyway.
        return CreateMutation(HydraManager, hydraRequest)
            ->Commit().Apply(BIND([] (TErrorOr<TMutationResponse> result) {
                return result.IsOK()
                    ? result.Value().Data
                    : CreateErrorResponseMessage(result);
            }));
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

    void HydraCommitSimpleTransaction(const TReqCommitSimpleTransaction& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error committing simple transaction (TransactionId: %v)",
                transactionId);
            return;
        }

        auto* commit = TransientCommitMap_.Find(transactionId);
        if (!commit) {
            // Commit could be missing (e.g. at followers or during recovery).
            // Let's recreate it since it's needed below in SetCommitSucceeded.
            commit = new TCommit(
                transactionId,
                mutationId,
                std::vector<TCellGuid>());
            TransientCommitMap_.Insert(transactionId, commit);
        }

        SetCommitSucceeded(commit, commitTimestamp);
        RemoveCommit(commit);
    }

    void HydraCommitDistributedTransactionPhaseOne(const TReqCommitDistributedTransactionPhaseOne& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request.participant_cell_guids());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());

        // Ensure commit existence.
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
            "Distributed commit phase one started "
            "(TransactionId: %v, ParticipantCellGuids: [%v], PrepareTimestamp: %v, CoordinatorCellGuid: %v)",
            transactionId,
            JoinToString(participantCellGuids),
            prepareTimestamp,
            coordinatorCellGuid);

        // Prepare at coordinator.
        try {
            // Any exception thrown here is caught below.
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                true,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing transaction commit at coordinator (TransactionId: %v)",
                transactionId);
            SetCommitFailed(commit, ex);
            RemoveCommit(commit);
            // Best effort, fire-and-forget.
            auto this_ = MakeStrong(this);
            EpochAutomatonInvoker_->Invoke(BIND([this, this_, transactionId] () {
                AbortTransaction(transactionId, false);
            }));
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Coordinator has prepared transaction (TransactionId: %v)",
            transactionId);

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

        YCHECK(!FindCommit(transactionId));

        TError error; // initially OK
        try {
            // Any exception thrown here is caught below and replied to the coordinator.
            TransactionManager_->PrepareTransactionCommit(
                transactionId,
                true,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error preparing transaction commit at participant (TransactionId: %v)",
                transactionId);
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
            LOG_DEBUG_UNLESS(IsRecovery(),
                "Invalid or expired transaction has been prepared, ignoring "
                "(TransactionId: %v, ParticipantCellGuid: %v)",
                transactionId,
                participantCellGuid);
            return;
        }

        auto error = FromProto<TError>(request.error());
        if (!error.IsOK()) {
            LOG_DEBUG_UNLESS(IsRecovery(), error, "Participant has failed to prepare (TransactionId: %v, ParticipantCellGuid: %v)",
                transactionId,
                participantCellGuid);

            SetCommitFailed(commit, error);

            // Transaction is already prepared at coordinator and (possibly) at some participants.
            // We _must_ forcefully abort it.
            try {
                TransactionManager_->AbortTransaction(transactionId, true);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error aborting transaction, ignored (TransactionId: %v)",
                    transactionId);
            }

            TReqHydraAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            ToProto(hydraRequest.mutable_mutation_id(), NullMutationId);
            hydraRequest.set_force(true);
            PostToParticipants(commit, hydraRequest);

            RemoveCommit(commit);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant has prepared transaction (TransactionId: %v, ParticipantCellGuid: %v)",
            transactionId,
            participantCellGuid);

        YCHECK(commit->PreparedParticipantCellGuids().insert(participantCellGuid).second);

        if (IsLeader()) {
            CheckForPhaseTwo(commit);
        }
    }

    void HydraCommitDistributedTransactionPhaseTwo(const TReqCommitDistributedTransactionPhaseTwo& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to start phase two for an invalid or expired transaction, ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        YCHECK(commit->IsDistributed());

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error committing transaction at coordinator (TransactionId: %v)",
                transactionId);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Coordinator has committed transaction (TransactionId: %v)",
            transactionId);

        // Commit at participants.
        {
            TReqCommitPreparedTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(commitTimestamp);
            PostToParticipants(commit, hydraRequest);
        }

        SetCommitSucceeded(commit, commitTimestamp);
        RemoveCommit(commit);
    }

    void HydraCommitPreparedTransaction(const TReqCommitPreparedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        YCHECK(!FindCommit(transactionId));

        try {
            // Any exception thrown here is caught below.
            TransactionManager_->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error committing transaction at participant (TransactionId: %v)",
                transactionId);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant has committed transaction (TransactionId: %v)",
            transactionId);
    }

    void HydraAbortTransaction(const TReqHydraAbortTransaction& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto force = request.force();

        try {
            // All exceptions thrown here are caught below.
            TransactionManager_->AbortTransaction(transactionId, force);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error aborting transaction, ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        auto* commit = FindCommit(transactionId);
        if (commit) {
            TReqHydraAbortTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            ToProto(hydraRequest.mutable_mutation_id(), NullMutationId);
            hydraRequest.set_force(true);
            PostToParticipants(commit, hydraRequest);

            auto error = TError("Transaction %v was aborted", transactionId);
            SetCommitFailed(commit, error);
            RemoveCommit(commit);
        }

        {
            TRspAbortTransaction response;
            auto responseMessage = CreateResponseMessage(response);

            auto* mutationContext = HydraManager->GetMutationContext();
            mutationContext->Response().Data = responseMessage;

            if (mutationId != NullMutationId) {
                ResponseKeeper_->EndRequest(mutationId, responseMessage);
            }
        }
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

    void RemoveCommit(TCommit* commit)
    {
        auto transactionId = commit->GetTransactionId();
        YCHECK(
            TransientCommitMap_.TryRemove(transactionId) ||
            PersistentCommitMap_.TryRemove(transactionId));
    }


    void SetCommitFailed(TCommit* commit, const TError& error)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), error, "Transaction commit failed (TransactionId: %v)",
            commit->GetTransactionId());

        auto responseMessage = CreateErrorResponseMessage(error);
        SetCommitResponse(commit, responseMessage);
    }

    void SetCommitSucceeded(TCommit* commit, TTimestamp commitTimestamp)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit succeeded (TransactionId: %v, CommitTimestamp: %v)",
            commit->GetTransactionId(),
            commitTimestamp);

        TRspCommitTransaction response;
        response.set_commit_timestamp(commitTimestamp);

        auto responseMessage = CreateResponseMessage(response);
        SetCommitResponse(commit, responseMessage);
    }

    void SetCommitResponse(TCommit* commit, TSharedRefArray responseMessage)
    {
        const auto& mutationId = commit->GetMutationId();
        if (mutationId != NullMutationId) {
            ResponseKeeper_->EndRequest(mutationId, responseMessage);
        }

        commit->SetResponseMessage(std::move(responseMessage));
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


    void GenerateCommitTimestamp(TCommit* commit)
    {
        TimestampProvider_->GenerateTimestamps()
            .Subscribe(BIND(&TImpl::OnCommitTimestampGenerated, MakeStrong(this), commit->GetTransactionId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnCommitTimestampGenerated(const TTransactionId& transactionId, TErrorOr<TTimestamp> timestampOrError)
    {
        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_DEBUG("Commit timestamp generated for an invalid or expired transaction, ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        if (!timestampOrError.IsOK()) {
            auto error = TError("Error generating commit timestamp")
                << timestampOrError;
            SetCommitFailed(commit, error);
            // If this is a distributed transaction then is already prepared at coordinator and
            // at all participants. We _must_ forcefully abort it.
            AbortTransaction(transactionId, commit->IsDistributed());
            RemoveCommit(commit);
            return;
        }

        auto timestamp = timestampOrError.Value();
        if (commit->IsDistributed()) {
            TReqCommitDistributedTransactionPhaseTwo hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_commit_timestamp(timestamp);
            CreateMutation(HydraManager, hydraRequest)
                ->Commit();
        } else {
            TReqCommitSimpleTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            ToProto(hydraRequest.mutable_mutation_id(), commit->GetMutationId());
            hydraRequest.set_commit_timestamp(timestamp);
            CreateMutation(HydraManager, hydraRequest)
                ->Commit();
        }
    }


    void CheckForPhaseTwo(TCommit* commit)
    {
        if (!commit->IsDistributed())
            return;

        if (commit->PreparedParticipantCellGuids().size() != commit->ParticipantCellGuids().size())
            // Some participants are not prepared yet.
            return;

        const auto& transactionId = commit->GetTransactionId();

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit phase two started (TransactionId: %v)",
            transactionId);

        GenerateCommitTimestamp(commit);
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
            CheckForPhaseTwo(commit);
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

TAsyncError TTransactionSupervisor::AbortTransaction(
    const TTransactionId& transactionId,
    bool force)
{
    return Impl_->AbortTransaction(
        transactionId,
        force);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
