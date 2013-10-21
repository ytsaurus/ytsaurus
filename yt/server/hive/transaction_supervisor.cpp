#include "stdafx.h"
#include "transaction_supervisor.h"
#include "config.h"
#include "transaction_manager.h"
#include "hive_manager.h"
#include "distributed_commit.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/ytree/attribute_helpers.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/hive/transaction_supervisor_service_proxy.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/entity_map.h>

#include <server/hive/transaction_supervisor.pb.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;
using namespace NHive::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor::TImpl
    : public TServiceBase
    , public NHydra::TCompositeAutomatonPart
{
public:
    TImpl(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        NRpc::IRpcServerPtr rpcServer,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager)
        : TServiceBase(
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            TServiceId(TTransactionSupervisorServiceProxy::GetServiceName(), hiveManager->GetSelfCellGuid()),
            HiveLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , Config(config)
        , RpcServer(rpcServer)
        , AutomatonInvoker(automatonInvoker)
        , HiveManager(hiveManager)
        , TransactionManager(transactionManager)
    {
        Automaton->RegisterPart(this);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartTransaction, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTransactionSimple, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTransactionDistributed, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this), nullptr));
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

private:
    typedef TImpl TThis;
    
    TTransactionSupervisorConfigPtr Config;
    IRpcServerPtr RpcServer;
    IInvokerPtr AutomatonInvoker;
    THiveManagerPtr HiveManager;
    ITransactionManagerPtr TransactionManager;

    NHydra::TEntityMap<TTransactionId, TDistributedCommit> CommitMap;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, StartTransaction)
    {
        CreateMutation(HydraManager, AutomatonInvoker)
            ->SetRequestData(context->GetRequestBody())
            ->SetType(context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraStartTransaction, MakeStrong(this), context, ConstRef(context->Request())))
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->OnError(CreateRpcErrorHandler(context))
            ->Commit();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitTransaction)
    {
        if (request->participant_cell_guids_size() == 0) {
            // Simple commit.
            TReqCommitTransactionSimple hydraRequest;
            hydraRequest.mutable_transaction_id()->CopyFrom(request->transaction_id());
            CreateMutation(HydraManager, AutomatonInvoker)
                ->SetRequestData(context->GetRequestBody())
                ->SetType(hydraRequest.GetTypeName())
                ->SetAction(BIND(&TImpl::HydraCommitTransactionSimple, MakeStrong(this), context, hydraRequest))
                ->OnSuccess(CreateRpcSuccessHandler(context))
                ->OnError(CreateRpcErrorHandler(context))
                ->Commit();
        } else {
            //// Two-phase commit.
            TReqCommitTransactionDistributed hydraRequest;
            hydraRequest.mutable_transaction_id()->CopyFrom(request->transaction_id());
            hydraRequest.mutable_participant_cell_guids()->MergeFrom(request->participant_cell_guids());
            // TODO(babenko): request timestamp
            hydraRequest.set_prepare_timestamp(0);
            CreateMutation(HydraManager, AutomatonInvoker)
                ->SetRequestData(context->GetRequestBody())
                ->SetType(hydraRequest.GetTypeName())
                ->SetAction(BIND(&TImpl::HydraCommitTransactionDistributed, MakeStrong(this), context, hydraRequest))
                ->OnError(CreateRpcErrorHandler(context))
                // NB: No success handler.
                ->Commit();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortTransaction)
    {
        CreateMutation(HydraManager, AutomatonInvoker)
            ->SetRequestData(context->GetRequestBody())
            ->SetType(context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraAbortTransaction, MakeStrong(this), context, ConstRef(context->Request())))
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->OnError(CreateRpcErrorHandler(context))
            ->Commit();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo("TransactionId: %s",
            ~ToString(transactionId));

        TransactionManager->PingTransaction(transactionId);

        context->Reply();
    }



    // Hydra handlers.

    void HydraStartTransaction(TCtxStartTransactionPtr context, const TReqStartTransaction& request)
    {
        auto transactionId =
            request.has_transaction_id()
            ? FromProto<TTransactionId>(request.transaction_id())
            : NullTransactionId;
        auto startTimestamp = TTimestamp(request.start_timestamp());
        auto parentTransactionId =
            request.has_parent_transaction_id()
            ? FromProto<TTransactionId>(request.parent_transaction_id())
            : NullTransactionId;
        auto timeout =
            request.has_timeout()
            ? MakeNullable(TDuration(request.timeout()))
            : Null;
        auto attributes =
            request.has_attributes()
            ? FromProto(request.attributes())
            : CreateEphemeralAttributes();

        if (context) {
            context->SetRequestInfo("TransactionId: %s, StartTimestamp: %" PRId64", ParentTransactionId: %s, Timeout: %s",
                ~ToString(transactionId),
                startTimestamp,
                ~ToString(parentTransactionId),
                ~ToString(timeout));
        }

        // TODO(babenko): error handling?
        auto startedTransactionId = TransactionManager->StartTransaction(
            transactionId,
            startTimestamp,
            parentTransactionId,
            ~attributes,
            timeout);

        if (context) {
            auto* response = &context->Response();
            ToProto(response->mutable_transaction_id(), startedTransactionId);
            context->SetResponseInfo("TransactionId: %s",
                ~ToString(startedTransactionId));
        }
    }

    void HydraCommitTransactionSimple(TCtxCommitTransactionPtr context, const TReqCommitTransactionSimple& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());

        if (context) {
            context->SetRequestInfo("TransactionId: %s",
                ~ToString(transactionId));
        }
        
        // TODO(babenko): error handling?
        // TODO(babenko): timestamp?
        TransactionManager->CommitTransaction(transactionId, 0);

        LOG_DEBUG_UNLESS(IsRecovery(), "Simple transaction committed (TransactionId: %s)",
            ~ToString(transactionId));
    }

    void HydraCommitTransactionDistributed(TCtxCommitTransactionPtr context, const TReqCommitTransactionDistributed& request)
    {
        try {
            auto transactionId = FromProto<TTransactionId>(request.transaction_id());
            auto participantCellGuids = FromProto<TCellGuid>(request.participant_cell_guids());
            auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
            if (context) {
                context->SetRequestInfo("TransactionId: %s, ParticipantCellGuids: [%s]",
                    ~ToString(transactionId),
                    ~JoinToString(participantCellGuids));
            }

            if (CommitMap.Find(transactionId)) {
                THROW_ERROR_EXCEPTION("Distributed commit is already started");
            }
            
            auto* commit = new TDistributedCommit(transactionId, participantCellGuids);
            CommitMap.Insert(transactionId, commit);

            LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit started (TransactionId: %s, ParticipantCellGuids: [%s], PrepareTimestamp: %" PRId64 ")",
                ~ToString(transactionId),
                ~JoinToString(participantCellGuids),
                prepareTimestamp);

            const auto& coordinatorCellGuid = HiveManager->GetSelfCellGuid();

            DoPrepareDistributedCommit(transactionId, prepareTimestamp, coordinatorCellGuid);

            TReqPrepareTransactionCommit prepareRequest;
            ToProto(prepareRequest.mutable_transaction_id(), transactionId);
            prepareRequest.set_prepare_timestamp(prepareTimestamp);
            ToProto(prepareRequest.mutable_coordinator_cell_guid(), coordinatorCellGuid);
            for (const auto& cellGuid : participantCellGuids) {
                auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
                HiveManager->PostMessage(mailbox, prepareRequest);
            }
        } catch (const std::exception& ex) {
            if (context) {
                context->Reply(ex);
            }
        }
    }

    void HydraAbortTransaction(TCtxAbortTransactionPtr context, const TReqAbortTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());

        if (context) {
            context->SetRequestInfo("TransactionId: %s",
                ~ToString(transactionId));
        }

        // TODO(babenko): error handling?
        TransactionManager->AbortTransaction(transactionId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)",
            ~ToString(transactionId));
    }

    void HydraPrepareTransactionCommit(const TReqPrepareTransactionCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto prepareTimestamp = TTimestamp(request.prepare_timestamp());
        auto coordinatorCellGuid = FromProto<TCellGuid>(request.coordinator_cell_guid());

        DoPrepareDistributedCommit(transactionId, prepareTimestamp, coordinatorCellGuid);

        TReqOnTransactionCommitPrepared onPreparedRequest;
        ToProto(onPreparedRequest.mutable_transaction_id(), transactionId);
        ToProto(onPreparedRequest.mutable_participant_cell_guid(), HiveManager->GetSelfCellGuid());
        auto* mailbox = HiveManager->GetOrCreateMailbox(coordinatorCellGuid);
        HiveManager->PostMessage(mailbox, onPreparedRequest);
    }

    void HydraOnTransactionCommitPrepared(const TReqOnTransactionCommitPrepared& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto participantCellGuid = FromProto<TCellGuid>(request.participant_cell_guid());

        // TODO(babenko): error handling?
        auto* commit = CommitMap.Get(transactionId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit prepared by participant (TransactionId: %s, ParticipantCellGuid: %s)",
            ~ToString(transactionId),
            ~ToString(participantCellGuid));

        YCHECK(commit->PreparedParticipantCellGuids().insert(participantCellGuid).second);

        if (commit->PreparedParticipantCellGuids().size() == commit->ParticipantCellGuids().size() && IsLeader()) {
            // TODO(babenko): request timestamp
            auto commitTimestamp = 0;

            LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit is fully prepared (TransactionId: %s, CommitTimestamp: %" PRId64 ")",
                ~ToString(transactionId),
                commitTimestamp);

            DoCommitDistributed(transactionId, commitTimestamp);

            TReqCommitPreparedTransaction commitRequest;
            ToProto(commitRequest.mutable_transaction_id(), transactionId);
            commitRequest.set_commit_timestamp(commitTimestamp);
            for (const auto& cellGuid : commit->ParticipantCellGuids()) {
                auto* mailbox = HiveManager->GetOrCreateMailbox(cellGuid);
                HiveManager->PostMessage(mailbox, commitRequest);
            }

            CommitMap.Remove(transactionId);
        }
    }

    void HydraCommitPreparedTransaction(const TReqCommitPreparedTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = TTimestamp(request.commit_timestamp());
        DoCommitDistributed(transactionId, commitTimestamp);
    }


    void DoPrepareDistributedCommit(
        const TTransactionId& transactionId,
        TTimestamp prepareTimestamp,
        const TCellGuid& coordinatorCellGuid)
    {
        // TODO(babenko): error handling?
        TransactionManager->PrepareTransactionCommit(transactionId, prepareTimestamp);

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed commit prepared (TransactionId: %s, CoordinatorCellGuid: %s)",
            ~ToString(transactionId),
            ~ToString(coordinatorCellGuid));
    }

    void DoCommitDistributed(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        // TODO(babenko): error handling
        TransactionManager->CommitTransaction(transactionId, commitTimestamp);

        LOG_DEBUG_UNLESS(IsRecovery(), "Distributed transaction committed (TransactionId: %s, CommitTimestamp: %" PRId64 ")",
            ~ToString(transactionId),
            commitTimestamp);
    }


    virtual bool ValidateSnapshotVersion(int version) override
    {
        return version == 1;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 1;
    }

    
    virtual void Clear() override
    {
        CommitMap.Clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        CommitMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        CommitMap.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        CommitMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        CommitMap.LoadValues(context);
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
    ITransactionManagerPtr transactionManager)
    : Impl(New<TImpl>(
        config,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton,
        hiveManager,
        transactionManager))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
