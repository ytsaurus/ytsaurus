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

#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

#include <ytlib/hydra/rpc_helpers.h>

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
using namespace NHydra;
using namespace NHive::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor::TImpl
    : public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        IServerPtr rpcServer,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager,
        ITimestampProviderPtr timestampProvider)
        : THydraServiceBase(
            hydraManager,
            automatonInvoker,
            TServiceId(TTransactionSupervisorServiceProxy::GetServiceName(), hiveManager->GetSelfCellGuid()),
            HiveLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , Config(config)
        , RpcServer(rpcServer)
        , HiveManager(hiveManager)
        , TransactionManager(transactionManager)
        , TimestampProvider(timestampProvider)
    {
        YCHECK(Config);
        YCHECK(RpcServer);
        YCHECK(HiveManager);
        YCHECK(TransactionManager);
        YCHECK(TimestampProvider);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartDistributedCommit, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraFinalizeDistributedCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPrepareTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnTransactionPrepared, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitPreparedTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortFailedTransaction, Unretained(this)));

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

        Automaton->RegisterPart(this);
    }

    void Start()
    {
        RpcServer->RegisterService(this);
    }

    void Stop()
    {
        RpcServer->UnregisterService(this);
    }


    TMutationPtr CreateAbortTransactionMutation(const TReqAbortTransaction& request)
    {
        return CreateMutation(HydraManager, request);
    }

private:
    TTransactionSupervisorConfigPtr Config;
    IServerPtr RpcServer;
    THiveManagerPtr HiveManager;
    ITransactionManagerPtr TransactionManager;
    ITimestampProviderPtr TimestampProvider;

    TEntityMap<TTransactionId, TCommit> DistributedCommitMap;
    TEntityMap<TTransactionId, TCommit> SimpleCommitMap;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitTransaction)
    {
        ValidateActiveLeader();

        auto mutationId = GetMutationId(context);
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request->participant_cell_guids());

        context->SetRequestInfo("TransactionId: %s, ParticipantCellGuids: [%s]",
            ~ToString(transactionId),
            ~JoinToString(participantCellGuids));

        auto prepareTimestamp = TimestampProvider->GetLatestTimestamp();

        if (participantCellGuids.empty()) {
            // Simple commit.
            auto* commit = FindCommit(transactionId);
            if (commit) {
                LOG_DEBUG("Waiting for simple commit to complete (TransactionId: %s)",
                    ~ToString(transactionId));
                ReplyWhenFinished(commit, context);
                return;
            }

            auto keptResponse = HydraManager->FindKeptResponse(mutationId);
            if (keptResponse) {
                LOG_DEBUG("Replying with kept response (TransactionId: %s)",
                    ~ToString(transactionId));
                context->Reply(keptResponse->Data);
                return;
            }

            commit = new TCommit(
                false,
                transactionId,
                mutationId,
                participantCellGuids);
            SimpleCommitMap.Insert(transactionId, commit);
            ReplyWhenFinished(commit, context);

            try {
                // Any exception thrown here is replied to the client.
                TransactionManager->PrepareTransactionCommit(
                    transactionId,
                    false,
                    prepareTimestamp);
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                LOG_DEBUG(error, "Simple commit has failed to prepare (TransactionId: %s)",
                    ~ToString(transactionId));
                SetCommitFailed(commit, error);
                return;
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Simple commit prepared (TransactionId: %s, PrepareTimestamp: %" PRIu64 ")",
                ~ToString(transactionId),
                prepareTimestamp);

            RunCommit(commit);
        } else {
            // Distributed commit.
            TReqStartDistributedCommit startCommitRequest;
            startCommitRequest.mutable_transaction_id()->Swap(request->mutable_transaction_id());
            ToProto(startCommitRequest.mutable_mutation_id(), mutationId);
            startCommitRequest.mutable_participant_cell_guids()->Swap(request->mutable_participant_cell_guids());
            startCommitRequest.set_prepare_timestamp(prepareTimestamp);
            CreateMutation(HydraManager, startCommitRequest)
                ->SetAction(BIND(&TImpl::HydraStartDistributedCommit, MakeStrong(this), context, startCommitRequest))
                ->Commit();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortTransaction)
    {
        ValidateActiveLeader();

        auto mutationId = GetMutationId(context);
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %s",
            ~ToString(transactionId));

        TransactionManager->PrepareTransactionAbort(transactionId);

        CreateMutation(HydraManager, *request)
            ->SetId(mutationId)
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->Commit();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingTransaction)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %s",
            ~ToString(transactionId));

        // Any exception thrown here is replied to the client.
        TransactionManager->PingTransaction(transactionId, *request);

        context->Reply();
    }


    // Hydra handlers.

    TRspAbortTransaction HydraAbortTransaction(const TReqAbortTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());

        // Any exception thrown here is replied to the client.
        TransactionManager->AbortTransaction(transactionId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)",
            ~ToString(transactionId));

        return TRspAbortTransaction();
    }

    void HydraStartDistributedCommit(TCtxCommitTransactionPtr context, const TReqStartDistributedCommit& request)
    {
        auto mutationId = FromProto<TMutationId>(request.mutation_id());
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request.participant_cell_guids());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());

        YCHECK(!SimpleCommitMap.Find(transactionId));

        auto* commit = DistributedCommitMap.Find(transactionId);
        if (commit) {
            if (context) {
                LOG_DEBUG("Waiting for distributed commit to complete (TransactionId: %s)",
                    ~ToString(transactionId));
                ReplyWhenFinished(commit, context);
            }
            return;
        }
            
        commit = new TCommit(
            true,
            transactionId,
            mutationId,
            participantCellGuids);
        DistributedCommitMap.Insert(transactionId, commit);

        if (context) {
            ReplyWhenFinished(commit, context);
        }

        const auto& coordinatorCellGuid = HiveManager->GetSelfCellGuid();

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit first phase started (TransactionId: %s, ParticipantCellGuids: [%s], CoordinatorCellGuid: %s)",
            ~ToString(transactionId),
            ~JoinToString(participantCellGuids),
            ~ToString(coordinatorCellGuid));

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
            TReqPrepareTransaction hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            hydraRequest.set_prepare_timestamp(prepareTimestamp);
            ToProto(hydraRequest.mutable_coordinator_cell_guid(), coordinatorCellGuid);
            PostToParticipants(commit, hydraRequest);
        }
    }

    void HydraPrepareTransaction(const TReqPrepareTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
        auto coordinatorCellGuid = FromProto<TCellGuid>(request.coordinator_cell_guid());

        TReqOnTransactionPrepared response;
        ToProto(response.mutable_transaction_id(), transactionId);
        ToProto(response.mutable_participant_cell_guid(), HiveManager->GetSelfCellGuid());

        try {
            // Any exception thrown here is replied to the coordinator.
            DoPrepareDistributed(
                transactionId,
                prepareTimestamp,
                coordinatorCellGuid,
                false);
        } catch (const std::exception& ex) {
            ToProto(response.mutable_error(), TError(ex));
        }

        PostToCoordinator(coordinatorCellGuid, response);
    }

    void HydraOnTransactionPrepared(const TReqOnTransactionPrepared& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuid = FromProto<TCellGuid>(request.participant_cell_guid());

        auto* commit = DistributedCommitMap.Find(transactionId);
        if (!commit) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Invalid or expired transaction has prepared, ignoring (TransactionId: %s)",
                ~ToString(transactionId));
            return;
        }

        if (request.has_error()) {
            auto error = FromProto(request.error());
            LOG_DEBUG_UNLESS(IsRecovery(), error, "Participant has failed to prepare (TransactionId: %s, ParticipantCellGuid: %s)",
                ~ToString(transactionId),
                ~ToString(participantCellGuid));
            SetCommitFailed(commit, error);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Participant has prepared (TransactionId: %s, ParticipantCellGuid: %s)",
            ~ToString(transactionId),
            ~ToString(participantCellGuid));

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

    void HydraAbortFailedTransaction(const TReqAbortFailedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        
        DoAbortFailed(transactionId);

        auto* commit = FindCommit(transactionId);
        if (commit) {
            RemoveCommit(commit);
        }
    }

    void HydraFinalizeDistributedCommit(const TReqFinalizeDistributedCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to finalize an invalid or expired transaction, ignoring (TransactionId: %s)",
                ~ToString(transactionId));
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
            TReqCommitPreparedTransaction commitRequest;
            ToProto(commitRequest.mutable_transaction_id(), transactionId);
            commitRequest.set_commit_timestamp(commitTimestamp);
            commitRequest.set_is_distributed(true);
            PostToParticipants(commit, commitRequest);
        }

        SetCommitCompleted(commit, commitTimestamp);
    }


    TCommit* FindCommit(const TTransactionId& transactionId)
    {
        TCommit* commit = nullptr;
        if (!commit) {
            commit = DistributedCommitMap.Find(transactionId);
        }
        if (!commit) {
            commit = SimpleCommitMap.Find(transactionId);
        }
        return commit;
    }

    void SetCommitFailed(TCommit* commit, const TError& error)
    {
        auto responseMessage = CreateErrorResponseMessage(error);
        SetCommitResult(commit, responseMessage);

        const auto& transactionId = commit->GetTransactionId();

        TReqAbortFailedTransaction abortFailedRequest;
        ToProto(abortFailedRequest.mutable_transaction_id(), transactionId);

        if (HydraManager->IsMutating()) {
            // Abort at coordinator.
            DoAbortFailed(transactionId);

            // Abort at participants.
            PostToParticipants(commit, abortFailedRequest);

            RemoveCommit(commit);
        } else {
            YCHECK(commit->ParticipantCellGuids().empty());
            CreateMutation(HydraManager, abortFailedRequest)
                ->Commit();
        }
    }

    void SetCommitCompleted(TCommit* commit, TTimestamp commitTimestamp)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "%s transaction commit completed (TransactionId: %s, CommitTimestamp: %" PRIu64 ")",
            commit->IsDistributed() ? "Distributed" : "Simple",
            ~ToString(commit->GetTransactionId()),
            commitTimestamp);

        TRspCommitTransaction response;
        response.set_commit_timestamp(commitTimestamp);
        
        auto responseMessage = CreateResponseMessage(response);
        SetCommitResult(commit, responseMessage);

        RemoveCommit(commit);
    }

    void SetCommitResult(TCommit* commit, TSharedRefArray result)
    {
        auto mutationId = commit->GetMutationId();
        if (HydraManager->IsMutating() && mutationId != NullMutationId) {
            HydraManager->RegisterKeptResponse(mutationId, TMutationResponse(result, true));
        }

        commit->SetResult(std::move(result));
    }

    void RemoveCommit(TCommit* commit)
    {
        if (commit->IsDistributed()) {
            DistributedCommitMap.Remove(commit->GetTransactionId());
        } else {
            SimpleCommitMap.Remove(commit->GetTransactionId());
        }
    }

    static void ReplyWhenFinished(TCommit* commit, TCtxCommitTransactionPtr context)
    {
        commit->GetResult().Subscribe(BIND([=] (TSharedRefArray message) {
            context->Reply(std::move(message));
        }));
    }


    template <class TMessage>
    void PostToParticipants(TCommit* commit, const TMessage& message)
    {
        for (const auto& cellGuid : commit->ParticipantCellGuids()) {
            auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
            HiveManager->PostMessage(mailbox, message);
        }
    }

    template <class TMessage>
    void PostToCoordinator(const TCellGuid& coordinatorCellGuid, const TMessage& message)
    {
        auto* mailbox = HiveManager->GetOrCreateMailbox(coordinatorCellGuid);
        HiveManager->PostMessage(mailbox, message);
    }



    void RunCommit(TCommit* commit)
    {
        TimestampProvider->GenerateTimestamps()
            .Subscribe(BIND(&TImpl::OnCommitTimestampGenerated, MakeStrong(this), commit->GetTransactionId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnCommitTimestampGenerated(
        const TTransactionId& transactionId,
        TErrorOr<TTimestamp> timestampOrError) 
    {
        auto* commit = FindCommit(transactionId);
        if (!commit) {
            LOG_DEBUG("Commit timestamp generated for an invalid or expired transaction, ignoring (TransactionId: %s)",
                ~ToString(transactionId));
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
            TReqFinalizeDistributedCommit finalizeRequest;
            ToProto(finalizeRequest.mutable_transaction_id(), transactionId);
            finalizeRequest.set_commit_timestamp(timestamp);
            CreateMutation(HydraManager, finalizeRequest)
                ->Commit();
        } else {
            TReqCommitPreparedTransaction commitRequest;
            ToProto(commitRequest.mutable_transaction_id(), transactionId);
            commitRequest.set_commit_timestamp(timestamp);
            commitRequest.set_is_distributed(false);
            CreateMutation(HydraManager, commitRequest)
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
            TransactionManager->PrepareTransactionCommit(
                transactionId,
                true,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Failed to prepare distributed commit (TransactionId: %s, CoordinatorCellGuid: %s, PrepareTimestamp: %" PRIu64 ")",
                ~ToString(transactionId),
                ~ToString(coordinatorCellGuid),
                prepareTimestamp);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Distirbuted commit is prepared by %s (TransactionId: %s, CoordinatorCellGuid: %s, PrepareTimestamp: %" PRIu64 ")",
            isCoordinator ? "coordinator" : "participant",
            ~ToString(transactionId),
            ~ToString(coordinatorCellGuid),
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
            TransactionManager->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error committing prepared transaction");
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "%s transaction committed %s(TransactionId: %s, CommitTimestamp: %" PRIu64 ")",
            isDistributed ? "Distributed" : "Simple",
            isDistributed ? (isCoordinator ? "by coordinator " : "by participant ") : "",
            ~ToString(transactionId),
            commitTimestamp);
    }

    void DoAbortFailed(const TTransactionId& transactionId)
    {
        try {
            // All exceptions thrown here are caught below and ignored.
            TransactionManager->AbortTransaction(transactionId);
            LOG_DEBUG_UNLESS(IsRecovery(), "Failed transaction aborted (TransactionId: %s)",
                ~ToString(transactionId));
        } catch (const std::exception) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Failed to abort failed transaction, ignoring (TransactionId: %s)",
                ~ToString(transactionId));
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit second phase started (TransactionId: %s)",
            ~ToString(transactionId));

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
        for (const auto& pair : DistributedCommitMap) {
            auto* commit = pair.second;
            CheckForSecondPhaseStart(commit);
        }
    }

    virtual void OnStopLeading() override
    {
        SimpleCommitMap.Clear();
    }


    virtual void Clear() override
    {
        DistributedCommitMap.Clear();
        SimpleCommitMap.Clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        DistributedCommitMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        DistributedCommitMap.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        DistributedCommitMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        DistributedCommitMap.LoadValues(context);
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionSupervisor::TTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IServerPtr rpcServer,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    THiveManagerPtr hiveManager,
    ITransactionManagerPtr transactionManager,
    ITimestampProviderPtr timestampProvider)
    : Impl_(New<TImpl>(
        config,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton,
        hiveManager,
        transactionManager,
        timestampProvider))
{ }

TTransactionSupervisor::~TTransactionSupervisor()
{ }

void TTransactionSupervisor::Start()
{
    Impl_->Start();
}

void TTransactionSupervisor::Stop()
{
    Impl_->Stop();
}

TMutationPtr TTransactionSupervisor::CreateAbortTransactionMutation(const TReqAbortTransaction& request)
{
    return Impl_->CreateAbortTransactionMutation(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
