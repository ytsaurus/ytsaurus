#include "stdafx.h"
#include "transaction_supervisor.h"
#include "config.h"
#include "transaction_manager.h"
#include "hive_manager.h"
#include "commit.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/hive/transaction_supervisor_service_proxy.h>
#include <ytlib/hive/timestamp_provider.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/entity_map.h>
#include <server/hydra/hydra_service.h>

#include <server/election/election_manager.h>

#include <server/hive/transaction_supervisor.pb.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;
using namespace NHive::NProto;
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
        IRpcServerPtr rpcServer,
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

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartTransaction, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTransactionSimple, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTransactionDistributed, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPrepareTransactionCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnTransactionCommitPrepared, Unretained(this)));
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
        RpcServer->RegisterService(this);
    }

    TMutationPtr CreateStartTransactionMutation(const TReqStartTransaction& request)
    {
        return CreateMutation(HydraManager, AutomatonInvoker, request);
    }

    TMutationPtr CreateCommitTransactionMutation(const TReqCommitTransaction& request)
    {
        return CreateMutation(HydraManager, AutomatonInvoker, request);
    }

    TMutationPtr CreateAbortTransactionMutation(const TReqAbortTransaction& request)
    {
        return CreateMutation(HydraManager, AutomatonInvoker, request);
    }

private:
    typedef TImpl TThis;
    
    TTransactionSupervisorConfigPtr Config;
    IRpcServerPtr RpcServer;
    THiveManagerPtr HiveManager;
    ITransactionManagerPtr TransactionManager;
    ITimestampProviderPtr TimestampProvider;

    TEntityMap<TTransactionId, TCommit> PersistentCommitMap;
    TEntityMap<TTransactionId, TCommit> TransientCommitMap;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, StartTransaction)
    {
        ValidateActiveLeader();

        CreateMutation(HydraManager, AutomatonInvoker, *request)
            ->SetAction(BIND(IgnoreResult(&TImpl::HydraStartTransaction), MakeStrong(this), context, ConstRef(*request)))
            ->Commit();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitTransaction)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request->participant_cell_guids());

        context->SetRequestInfo("TransactionId: %s, ParticipantCellGuids: [%s]",
            ~ToString(transactionId),
            ~JoinToString(participantCellGuids));

        auto* commit = FindCommit(transactionId);
        if (commit) {
            ScheduleCommitReply(context, commit);
            return;
        }

        auto prepareTimestamp = TimestampProvider->GetLatestTimestamp();

        if (request->participant_cell_guids_size() == 0) {
            commit = new TCommit(false, transactionId, participantCellGuids);
            TransientCommitMap.Insert(transactionId, commit);

            // Simple commit.
            try {
                // Any exception thrown here is replied to the client.
                TransactionManager->PrepareTransactionCommit(
                    transactionId,
                    false,
                    prepareTimestamp);
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                LOG_DEBUG_UNLESS(IsRecovery(), error, "Simple commit has failed to prepare (TransactionId: %s)",
                    ~ToString(transactionId));
                SetCommitFailed(transactionId, commit, error);
                throw;
            }

            ScheduleCommitReply(context, commit);

            LOG_DEBUG_UNLESS(IsRecovery(), "Simple commit prepared (TransactionId: %s, PrepareTimestamp: %" PRId64 ")",
                ~ToString(transactionId),
                prepareTimestamp);

            if (!GenerateCommitTimestamp(commit))
                return;

            TReqCommitTransactionSimple hydraRequest;
            hydraRequest.mutable_transaction_id()->CopyFrom(request->transaction_id());
            hydraRequest.set_commit_timestamp(commit->GetCommitTimestamp());
            CreateMutation(HydraManager, AutomatonInvoker, hydraRequest)
                ->Commit();
        } else {
            // Distributed commit.
            TReqCommitTransactionDistributed hydraRequest;
            hydraRequest.mutable_transaction_id()->CopyFrom(request->transaction_id());
            hydraRequest.mutable_participant_cell_guids()->MergeFrom(request->participant_cell_guids());
            hydraRequest.set_prepare_timestamp(prepareTimestamp);
            CreateMutation(HydraManager, AutomatonInvoker, hydraRequest)
                ->SetAction(BIND(&TImpl::HydraCommitTransactionDistributed, MakeStrong(this), context, hydraRequest))
                ->Commit();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortTransaction)
    {
        ValidateActiveLeader();

        CreateMutation(HydraManager, AutomatonInvoker, *request)
            ->SetAction(BIND(&TImpl::HydraAbortTransaction, MakeStrong(this), context, ConstRef(*request)))
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

    void HydraStartTransaction(TCtxStartTransactionPtr context, const TReqStartTransaction& request)
    {
        auto startTimestamp = TTimestamp(request.start_timestamp());

        if (context) {
            context->SetRequestInfo("StartTimestamp: %" PRId64,
                startTimestamp);
        }

        TTransactionId transactionId;
        try {
            // Any exception thrown here is replied to the client.
            transactionId = TransactionManager->StartTransaction(startTimestamp, request);
        } catch (const std::exception& ex) {
            if (context) {
                context->Reply(ex);
            }
            return;
        }

        if (context) {
            auto& response = context->Response();
            ToProto(response.mutable_transaction_id(), transactionId);
            context->SetResponseInfo("TransactionId: %s",
                ~ToString(transactionId));
            context->Reply();
        }
    }

    void HydraCommitTransactionSimple(const TReqCommitTransactionSimple& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());

        // Commit could be missing (e.g. at followers).
        auto* commit = FindCommit(transactionId);

        try {
            // Any exception thrown here is caught below.
            TransactionManager->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            SetCommitFailed(transactionId, commit, ex);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Simple transaction committed (TransactionId: %s, CommitTimestamp: %" PRId64 ")",
            ~ToString(transactionId),
            commitTimestamp);

        if (commit) {
            SetCommitSucceded(commit);
        }
    }

    void HydraCommitTransactionDistributed(TCtxCommitTransactionPtr context, const TReqCommitTransactionDistributed& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuids = FromProto<TCellGuid>(request.participant_cell_guids());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
        if (context) {
            context->SetRequestInfo("TransactionId: %s, ParticipantCellGuids: [%s]",
                ~ToString(transactionId),
                ~JoinToString(participantCellGuids));
        }

        YCHECK(!TransientCommitMap.Find(transactionId));

        auto* commit = PersistentCommitMap.Find(transactionId);
        if (commit) {
            if (context) {
                ScheduleCommitReply(context, commit);
                return;
            }
            return;
        }
            
        commit = new TCommit(true, transactionId, participantCellGuids);
        PersistentCommitMap.Insert(transactionId, commit);

        if (context) {
            ScheduleCommitReply(context, commit);
        }

        const auto& coordinatorCellGuid = HiveManager->GetSelfCellGuid();

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit started (TransactionId: %s, ParticipantCellGuids: [%s])",
            ~ToString(transactionId),
            ~JoinToString(participantCellGuids));

        try {
            // Any exception thrown here is caught below.
            DoPrepareDistributedCommit(transactionId, prepareTimestamp, coordinatorCellGuid);
        } catch (const std::exception& ex) {
            SetCommitFailed(transactionId, commit, ex);
            return;
        }

        // Schedule preparation at other participants.
        TReqPrepareTransactionCommit prepareRequest;
        ToProto(prepareRequest.mutable_transaction_id(), transactionId);
        prepareRequest.set_prepare_timestamp(prepareTimestamp);
        ToProto(prepareRequest.mutable_coordinator_cell_guid(), coordinatorCellGuid);
        for (const auto& cellGuid : participantCellGuids) {
            auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
            HiveManager->PostMessage(mailbox, prepareRequest);
        }
    }

    void HydraAbortTransaction(TCtxAbortTransactionPtr context, const TReqAbortTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());

        if (context) {
            context->SetRequestInfo("TransactionId: %s",
                ~ToString(transactionId));
        }

        try {
            // Any exception thrown here is replied to the client.
            TransactionManager->AbortTransaction(transactionId);
        } catch (const std::exception& ex) {
            if (context) {
                context->Reply(ex);
            }
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)",
            ~ToString(transactionId));

        if (context) {
            context->Reply();
        }
    }

    void HydraPrepareTransactionCommit(const TReqPrepareTransactionCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
        auto coordinatorCellGuid = FromProto<TCellGuid>(request.coordinator_cell_guid());

        TReqOnTransactionCommitPrepared onPreparedRequest;
        ToProto(onPreparedRequest.mutable_transaction_id(), transactionId);
        ToProto(onPreparedRequest.mutable_participant_cell_guid(), HiveManager->GetSelfCellGuid());

        try {
            // Any exception thrown here is replied to the coordinator.
            DoPrepareDistributedCommit(transactionId, prepareTimestamp, coordinatorCellGuid);
        } catch (const std::exception& ex) {
            ToProto(onPreparedRequest.mutable_error(), TError(ex));
        }

        auto* mailbox = HiveManager->GetOrCreateMailbox(coordinatorCellGuid);
        HiveManager->PostMessage(mailbox, onPreparedRequest);
    }

    void HydraOnTransactionCommitPrepared(const TReqOnTransactionCommitPrepared& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuid = FromProto<TCellGuid>(request.participant_cell_guid());

        auto* commit = PersistentCommitMap.Find(transactionId);
        if (!commit) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Invalid or expired transaction commit has prepared, ignoring (TransactionId: %s, ParticipantCellGuid: %s)",
                ~ToString(transactionId),
                ~ToString(participantCellGuid));
            return;
        }

        if (request.has_error()) {
            auto error = FromProto(request.error());
            LOG_DEBUG_UNLESS(IsRecovery(), error, "Distributed commit has failed to prepare (TransactionId: %s, ParticipantCellGuid: %s)",
                ~ToString(transactionId),
                ~ToString(participantCellGuid));
            SetCommitFailed(transactionId, commit, error);
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit prepared by participant (TransactionId: %s, ParticipantCellGuid: %s)",
            ~ToString(transactionId),
            ~ToString(participantCellGuid));

        YCHECK(commit->PreparedParticipantCellGuids().insert(participantCellGuid).second);

        if (IsLeader() && commit->PreparedParticipantCellGuids().size() == commit->ParticipantCellGuids().size()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit is fully prepared (TransactionId: %s)",
                ~ToString(transactionId));

            if (!GenerateCommitTimestamp(commit))
                return;

            DoCommitDistributed(transactionId, commit->GetCommitTimestamp());

            TReqCommitPreparedTransaction commitRequest;
            ToProto(commitRequest.mutable_transaction_id(), transactionId);
            commitRequest.set_commit_timestamp(commit->GetCommitTimestamp());
            for (const auto& cellGuid : commit->ParticipantCellGuids()) {
                auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
                HiveManager->PostMessage(mailbox, commitRequest);
            }

            SetCommitSucceded(commit);
        }
    }

    void HydraCommitPreparedTransaction(const TReqCommitPreparedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());
        DoCommitDistributed(transactionId, commitTimestamp);
    }

    void HydraAbortFailedTransaction(const TReqAbortFailedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        DoAbortFailed(transactionId);
    }


    TCommit* FindCommit(const TTransactionId& transactionId)
    {
        TCommit* commit = nullptr;
        if (!commit) {
            commit = PersistentCommitMap.Find(transactionId);
        }
        if (!commit) {
            commit = TransientCommitMap.Find(transactionId);
        }
        return commit;
    }

    void SetCommitFailed(const TTransactionId& transactionId, TCommit* commit, const TError& error)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), error, "Transaction commit failed (TransactionId: %s)",
            ~ToString(transactionId));

        if (!commit)
            return;

        commit->SetResult(error);

        TReqAbortFailedTransaction request;
        ToProto(request.mutable_transaction_id(), commit->GetTransactionId());

        if (HydraManager->IsMutating()) {
            DoAbortFailed(commit->GetTransactionId());           
            for (const auto& cellGuid : commit->ParticipantCellGuids()) {
                auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
                HiveManager->PostMessage(mailbox, request);
            }
            RemoveCommit(commit);
        } else {
            YCHECK(commit->ParticipantCellGuids().empty());
            CreateMutation(HydraManager, AutomatonInvoker, request)
                ->Commit();
        }
    }

    void SetCommitSucceded(TCommit* commit)
    {
        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit succeeded (TransactionId: %s)",
            ~ToString(commit->GetTransactionId()));

        commit->SetResult(TError());
        RemoveCommit(commit);
    }

    void RemoveCommit(TCommit* commit)
    {
        if (commit->GetPersistent()) {
            PersistentCommitMap.Remove(commit->GetTransactionId());
        } else {
            TransientCommitMap.Remove(commit->GetTransactionId());
        }
    }

    void ScheduleCommitReply(TCtxCommitTransactionPtr context, TCommit* commit)
    {
        commit->GetResult().Subscribe(BIND([=] (TError error) {
            if (error.IsOK()) {
                auto& response = context->Response();
                response.set_commit_timestamp(commit->GetCommitTimestamp());
                context->Reply();
            } else {
                context->Reply(error);
            }
        }));
    }

    bool GenerateCommitTimestamp(TCommit* commit)
    {
        auto commitTimestampOrError = WaitFor(TimestampProvider->GenerateNewTimestamp(), EpochAutomatonInvoker);
        if (!commitTimestampOrError.IsOK()) {
            auto error = TError("Error generating commit timestamp")
                << commitTimestampOrError;
            LOG_ERROR(error);
            SetCommitFailed(commit->GetTransactionId(), commit, error);
            return false;
        }

        commit->SetCommitTimestamp(commitTimestampOrError.GetValue());
        return true;
    }


    void DoPrepareDistributedCommit(
        const TTransactionId& transactionId,
        TTimestamp prepareTimestamp,
        const TCellGuid& coordinatorCellGuid)
    {
        // Any exception thrown here is propagated to the caller.
        try {
            TransactionManager->PrepareTransactionCommit(
                transactionId,
                true,
                prepareTimestamp);
        } catch (const std::exception& ex) {
            LOG_DEBUG_UNLESS(IsRecovery(), ex, "Distributed commit has failed to prepare (TransactionId: %s, CoordinatorCellGuid: %s, PrepareTimestamp: %" PRId64 ")",
                ~ToString(transactionId),
                ~ToString(coordinatorCellGuid),
                prepareTimestamp);
            throw;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit prepared (TransactionId: %s, CoordinatorCellGuid: %s, PrepareTimestamp: %" PRId64 ")",
            ~ToString(transactionId),
            ~ToString(coordinatorCellGuid),
            prepareTimestamp);
    }

    void DoCommitDistributed(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        try {
            // Cannot throw since it has already prepared successfully.
            TransactionManager->CommitTransaction(transactionId, commitTimestamp);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error committing prepared transaction");
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed transaction committed (TransactionId: %s, CommitTimestamp: %" PRId64 ")",
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

    
    virtual bool ValidateSnapshotVersion(int version) override
    {
        return version == 1;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 1;
    }


    virtual void OnStopLeading() override
    {
        TransientCommitMap.Clear();
    }

    virtual void Clear() override
    {
        PersistentCommitMap.Clear();
        TransientCommitMap.Clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        PersistentCommitMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        PersistentCommitMap.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        PersistentCommitMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        PersistentCommitMap.LoadValues(context);
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionSupervisor::TTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IRpcServerPtr rpcServer,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    THiveManagerPtr hiveManager,
    ITransactionManagerPtr transactionManager,
    ITimestampProviderPtr timestampProvider)
    : Impl(New<TImpl>(
        config,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton,
        hiveManager,
        transactionManager,
        timestampProvider))
{ }

TMutationPtr TTransactionSupervisor::CreateStartTransactionMutation(const TReqStartTransaction& request)
{
    return Impl->CreateStartTransactionMutation(request);
}

TMutationPtr TTransactionSupervisor::CreateCommitTransactionMutation(const TReqCommitTransaction& request)
{
    return Impl->CreateCommitTransactionMutation(request);
}

TMutationPtr TTransactionSupervisor::CreateAbortTransactionMutation(const TReqAbortTransaction& request)
{
    return Impl->CreateAbortTransactionMutation(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
