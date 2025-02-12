#include "transaction_manager.h"

#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "serialize.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_manager_detail.h>

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/server/node/tablet_node/transaction_manager.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/tablet_client/proto/tablet_service.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <util/generic/cast.h>

#include <set>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NHydra;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NClusterNode;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTransactionSupervisor;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TTabletAutomatonPart
    , public ITransactionManager
    , public TTransactionManagerBase<TTransaction>
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionStarted);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*, bool), TransactionPrepared);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionCommitted);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionCoarselySerialized);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionPerRowSerialized);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), BeforeTransactionCoarselySerialized);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionAborted);
    DEFINE_SIGNAL_OVERRIDE(void(TTimestamp), TransactionBarrierHandled);
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionTransientReset);

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        ITransactionManagerHostPtr host,
        TClusterTag clockClusterTag,
        ITransactionLeaseTrackerPtr transactionLeaseTracker)
        : TTabletAutomatonPart(
            host->GetCellId(),
            host->GetSimpleHydraManager(),
            host->GetAutomaton(),
            host->GetAutomatonInvoker(),
            host->GetMutationForwarder())
        , Host_(host)
        , Config_(config)
        , LeaseTracker_(std::move(transactionLeaseTracker))
        , NativeCellTag_(host->GetNativeCellTag())
        , NativeConnection_(host->GetNativeConnection())
        , ClockClusterTag_(clockClusterTag)
        , TransactionSerializationLagTimer_(TabletNodeProfiler()
            .WithTag("cell_id", ToString(host->GetCellId()))
            .Timer("/transaction_serialization_lag"))
        , AbortTransactionIdPool_(Config_->MaxAbortedTransactionPoolSize)
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(host->GetAutomatonInvoker(), AutomatonThread);

        Logger = TabletNodeLogger().WithTag("CellId: %v", host->GetCellId());

        YT_LOG_INFO("Set transaction manager clock cluster tag (ClockClusterTag: %v)",
            ClockClusterTag_);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND_NO_PROPAGATE(&TTransactionManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND_NO_PROPAGATE(&TTransactionManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND_NO_PROPAGATE(&TTransactionManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionManager.Values",
            BIND_NO_PROPAGATE(&TTransactionManager::SaveValues, Unretained(this)));

        // COMPAT(babenko)
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraRegisterTransactionActions, Unretained(this)), {"NYT.NTabletNode.NProto.TReqRegisterTransactionActions"});
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraHandleTransactionBarrier, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraExternalizeTransaction, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraPrepareExternalizedTransaction, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraCommitExternalizedTransaction, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraAbortExternalizedTransaction, Unretained(this)));

        OrchidService_ = IYPathService::FromProducer(BIND(&TTransactionManager::BuildOrchidYson, MakeWeak(this)), TDuration::Seconds(1))
            ->Via(Host_->GetGuardedAutomatonInvoker());
    }

    void PerRowSerialized(TTransaction* transaction) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto commitTimestamp = transaction->GetCommitTimestamp();
        auto transactionId = transaction->GetId();
        YT_LOG_DEBUG("Transaction prs serialized (TransactionId: %v, CommitTimestamp: %v)",
            transaction->GetId(),
            commitTimestamp);

        if (transaction->CoarseSerializingTabletIds().empty()){
            // NB: Either they are already serialized and action below is noop
            // or there were no coarsely serialized tablets and transition to serialized is ok.
            YT_ASSERT(transaction->GetPersistentState() == ETransactionState::Committed ||
                transaction->GetPersistentState() == ETransactionState::Serialized);
            transaction->SetPersistentState(ETransactionState::Serialized);
        }

        TransactionPerRowSerialized_.Fire(transaction);

        if (transaction->CoarseSerializingTabletIds().empty() && transaction->PerRowSerializingTabletIds().empty()) {
            transaction->SetFinished();

            PersistentTransactionMap_.Remove(transactionId);
        }
    }

    TTransaction* FindPersistentTransaction(TTransactionId transactionId) override
    {
        return PersistentTransactionMap_.Find(transactionId);
    }

    TTransaction* GetPersistentTransaction(TTransactionId transactionId) override
    {
        return PersistentTransactionMap_.Get(transactionId);
    }

    TTransaction* GetPersistentTransactionOrThrow(TTransactionId transactionId, TGuid externalizationToken = {})
    {
        TTransaction* transaction = nullptr;

        if (externalizationToken) {
            transaction = ExternalizedTransactionMap_.Find({transactionId, externalizationToken});
        } else {
            transaction = PersistentTransactionMap_.Find(transactionId);
        }

        if (transaction) {
            return transaction;
        }

        auto error = CreateNoSuchTransactionError(transactionId);
        if (externalizationToken) {
            error <<= TErrorAttribute("externalization_token", externalizationToken);
        }
        THROW_ERROR error;
    }

    TTransaction* FindTransaction(TTransactionId transactionId, TGuid externalizationToken = {})
    {
        if (externalizationToken) {
            if (auto* transaction = ExternalizedTransactionMap_.Find({transactionId, externalizationToken})) {
                return transaction;
            }
            return nullptr;
        }

        if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        return nullptr;
    }

    TTransaction* GetTransactionOrThrow(TTransactionId transactionId, TGuid externalizationToken = {})
    {
        auto* transaction = FindTransaction(transactionId, externalizationToken);

        if (!transaction) {
            auto error = CreateNoSuchTransactionError(transactionId);
            if (externalizationToken) {
                error <<= TErrorAttribute("externalization_token", externalizationToken);
            }
            THROW_ERROR error;
        }
        return transaction;
    }

    TTransaction* GetOrCreateTransactionOrThrow(
        TTransactionId transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient,
        TGuid externalizationToken = {}) override
    {
        YT_VERIFY(!externalizationToken || !transient);

        if (externalizationToken) {
            if (auto* transaction = ExternalizedTransactionMap_.Find({transactionId, externalizationToken})) {
                return transaction;
            }
        } else {
            if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
                return transaction;
            }
            if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
                return transaction;
            }

            if (transient && AbortTransactionIdPool_.IsRegistered(transactionId)) {
                THROW_ERROR_EXCEPTION("Abort was requested for transaction %v",
                    transactionId);
            }
        }

        std::unique_ptr<TExternalizedTransaction> externalizedTransactionHolder;
        std::unique_ptr<TTransaction> transactionHolder;
        TTransaction* transaction;

        if (externalizationToken) {
            externalizedTransactionHolder = std::make_unique<TExternalizedTransaction>(transactionId, externalizationToken);
            transaction = externalizedTransactionHolder.get();
        } else {
            transactionHolder = std::make_unique<TTransaction>(transactionId);
            transaction = transactionHolder.get();
        }

        transaction->SetForeign(CellTagFromId(transactionId) != NativeCellTag_);
        transaction->SetTimeout(timeout);
        transaction->SetStartTimestamp(startTimestamp);
        transaction->SetPersistentState(ETransactionState::Active);
        transaction->SetTransient(transient);
        transaction->AuthenticationIdentity() = NRpc::GetCurrentAuthenticationIdentity();

        ValidateNotDecommissioned(transaction);

        if (externalizationToken) {
            ExternalizedTransactionMap_.Insert(
                {transactionId, externalizationToken},
                std::move(externalizedTransactionHolder));
        } else {
            auto& map = transient ? TransientTransactionMap_ : PersistentTransactionMap_;
            map.Insert(transactionId, std::move(transactionHolder));

            if (IsLeader()) {
                CreateLease(transaction);
            }
        }

        YT_LOG_DEBUG("Transaction started (TransactionId: %v, StartTimestamp: %v, StartTime: %v, "
            "Timeout: %v, Transient: %v)",
            FormatTransactionId(transactionId, externalizationToken),
            startTimestamp,
            TimestampToInstant(startTimestamp).first,
            timeout,
            transient);

        return transaction;
    }

    TTransaction* MakeTransactionPersistentOrThrow(TTransactionId transactionId) override
    {
        if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
            ValidateNotDecommissioned(transaction);

            transaction->SetTransient(false);
            if (IsLeader()) {
                CreateLease(transaction);
            }
            auto transactionHolder = TransientTransactionMap_.Release(transactionId);
            PersistentTransactionMap_.Insert(transactionId, std::move(transactionHolder));
            YT_LOG_DEBUG("Transaction became persistent (TransactionId: %v)",
                transactionId);
            return transaction;
        }

        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            YT_VERIFY(!transaction->GetTransient());
            return transaction;
        }

        YT_ABORT();
    }

    std::vector<TTransaction*> GetTransactions() override
    {
        std::vector<TTransaction*> transactions;
        for (auto [transactionId, transaction] : TransientTransactionMap_) {
            transactions.push_back(transaction);
        }
        for (auto [transactionId, transaction] : PersistentTransactionMap_) {
            transactions.push_back(transaction);
        }
        return transactions;
    }

    TFuture<void> RegisterTransactionActions(
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        ::google::protobuf::RepeatedPtrField<NTransactionClient::NProto::TTransactionActionData>&& actions) override
    {
        NTabletClient::NProto::TReqRegisterTransactionActions request;
        ToProto(request.mutable_transaction_id(), transactionId);
        request.set_transaction_start_timestamp(transactionStartTimestamp);
        request.set_transaction_timeout(ToProto(transactionTimeout));
        request.set_signature(signature);
        request.mutable_actions()->Swap(&actions);
        NRpc::WriteAuthenticationIdentityToProto(&request, NRpc::GetCurrentAuthenticationIdentity());

        auto mutation = CreateMutation(HydraManager_, request);
        mutation->SetCurrentTraceContext();
        return mutation->CommitAndLog(Logger).AsVoid();
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    // ITransactionManager implementation.

    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& /*prerequisiteTransactionIds*/,
        const std::vector<TCellId>& /*cellIdsToSyncWith*/) override
    {
        return VoidFuture;
    }

    void PrepareTransactionCommit(
        TTransactionId transactionId,
        const TTransactionPrepareOptions& options) override
    {
        PrepareTransactionCommit(transactionId, {}, options);
    }

    void PrepareTransactionCommit(
        TTransactionId transactionId,
        TGuid externalizationToken,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        ValidateTimestampClusterTag(
            transactionId,
            options.PrepareTimestampClusterTag,
            options.PrepareTimestamp,
            /*canThrow*/ true);

        auto persistent = options.Persistent;

        TTransaction* transaction;
        ETransactionState state;
        TTransactionSignature prepareSignature;
        if (persistent) {
            transaction = GetPersistentTransactionOrThrow(transactionId, externalizationToken);
            state = transaction->GetPersistentState();
            prepareSignature = transaction->PersistentPrepareSignature();
        } else {
            transaction = GetTransactionOrThrow(transactionId);
            state = transaction->GetTransientState();
            prepareSignature = transaction->TransientPrepareSignature();
        }

        // Allow preparing transactions in Active and TransientCommitPrepared (for persistent mode) states.
        if (state != ETransactionState::Active &&
            !(persistent && state == ETransactionState::TransientCommitPrepared))
        {
            transaction->ThrowInvalidState();
        }

        if (!transaction->IsExternalizedToThisCell() && prepareSignature != FinalTransactionSignature) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::IncompletePrepareSignature,
                "Transaction %v is incomplete: expected prepare signature %x, actual signature %x",
                transactionId,
                FinalTransactionSignature,
                prepareSignature);
        }

        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&transaction->AuthenticationIdentity());

        if (persistent) {
            const auto* context = GetCurrentMutationContext();
            transaction->SetPrepareRevision(context->GetVersion().ToRevision());
        }

        if (state == ETransactionState::Active) {
            YT_VERIFY(transaction->GetPrepareTimestamp() == NullTimestamp);
            transaction->SetPrepareTimestamp(options.PrepareTimestamp);
            RegisterPrepareTimestamp(transaction);

            if (persistent) {
                transaction->SetPersistentState(ETransactionState::PersistentCommitPrepared);
            } else {
                transaction->SetTransientState(ETransactionState::TransientCommitPrepared);
            }

            TransactionPrepared_.Fire(transaction, persistent);

            // COMPAT(kvk1920)
            bool requireLegacyBehavior = false;
            if (const auto* mutationContext = NHydra::TryGetCurrentMutationContext()) {
                auto currentReign = static_cast<ETabletReign>(mutationContext->Request().Reign);
                if (currentReign < ETabletReign::SaneTxActionAbort) {
                    requireLegacyBehavior = true;
                }
            }
            RunPrepareTransactionActions(transaction, options, requireLegacyBehavior);

            YT_LOG_DEBUG("Transaction commit prepared (TransactionId: %v, Persistent: %v, "
                "PrepareTimestamp: %v@%v)",
                FormatTransactionId(transactionId, externalizationToken),
                persistent,
                options.PrepareTimestamp,
                options.PrepareTimestampClusterTag);
        }

        if (persistent) {
            if (transaction->IsExternalizedFromThisCell()) {
                YT_LOG_DEBUG("Forwarding externalized transaction prepare "
                    "(TransactionId: %v, ExternalizerTabletIds: %v, PrepareTimestamp: %v)",
                    transaction->GetId(),
                    transaction->ExternalizerTablets(),
                    options.PrepareTimestamp);
            }

            // NB: Forwarding must happen after transaction actions are run because
            // prepare may fail locally.
            ForwardTransactionIfExternalized(
                transaction,
                NProto::TReqPrepareExternalizedTransaction{},
                options);
        }
    }

    void PrepareTransactionAbort(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        AbortTransactionIdPool_.Register(transactionId);

        auto* transaction = GetTransactionOrThrow(transactionId);

        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&transaction->AuthenticationIdentity());

        if (transaction->GetTransientState() != ETransactionState::Active && !options.Force) {
            transaction->ThrowInvalidState();
        }

        if (transaction->GetTransientState() == ETransactionState::Active) {
            transaction->SetTransientState(ETransactionState::TransientAbortPrepared);

            YT_LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
                transactionId);
        }
    }

    void CommitTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionCommitOptions& options) override
    {
        CommitTransaction(transactionId, {}, options);
    }

    void CommitTransaction(
        TTransactionId transactionId,
        TGuid externalizationToken,
        const NTransactionSupervisor::TTransactionCommitOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto* transaction = GetTransactionOrThrow(transactionId, externalizationToken);
        if (transaction->GetTransient()) {
            YT_VERIFY(!externalizationToken);

            YT_LOG_ALERT("Attempted to commit transient transaction, reporting error "
                "(TransactionId: %v, State: %v)",
                transactionId,
                transaction->GetTransientState());

            // Will throw NoSuchTransaction error.
            Y_UNUSED(GetPersistentTransactionOrThrow(transactionId));
            YT_ABORT();
        }

        if (transaction->IsExternalizedToThisCell() || transaction->CommitSignature() == FinalTransactionSignature) {
            DoCommitTransaction(transaction, options);
        } else {
            transaction->SetPersistentState(ETransactionState::CommitPending);
            transaction->CommitOptions() = options;

            YT_LOG_DEBUG(
                "Transaction commit signature is incomplete, waiting for additional data "
                "(TransactionId: %v, CommitSignature: %x, ExpectedSignature: %x)",
                FormatTransactionId(transactionId, externalizationToken),
                transaction->CommitSignature(),
                FinalTransactionSignature);
        }
    }

    void DoCommitTransaction(
        TTransaction* transaction,
        const NTransactionSupervisor::TTransactionCommitOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        // Make a copy, transaction may die.
        auto transactionId = transaction->GetId();
        auto externalizationToken = transaction->GetExternalizationToken();
        auto identity = transaction->AuthenticationIdentity();
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            YT_LOG_DEBUG("Transaction is already committed (TransactionId: %v)",
                transactionId);
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared &&
            state != ETransactionState::CommitPending)
        {
            transaction->ThrowInvalidState();
        }

        if (IsLeader() && !externalizationToken) {
            CloseLease(transaction);
        }

        ValidateTimestampClusterTag(
            transactionId,
            options.CommitTimestampClusterTag,
            transaction->GetPrepareTimestamp(),
            /*canThrow*/ false);

        if (!transaction->IsExternalizedToThisCell()) {
            YT_LOG_ALERT_UNLESS(transaction->PersistentPrepareSignature() == FinalTransactionSignature,
                "Transaction signature is incomplete during commit "
                "(TransactionId: %v, PrepareSignature: %x, ExpectedSignature: %x)",
                transaction->GetId(),
                transaction->PersistentPrepareSignature(),
                FinalTransactionSignature);
        }

        transaction->SetCommitTimestamp(options.CommitTimestamp);
        transaction->SetCommitTimestampClusterTag(options.CommitTimestampClusterTag);
        transaction->SetPersistentState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);
        RunCommitTransactionActions(transaction, options);

        YT_LOG_DEBUG("Transaction committed (TransactionId: %v, CommitTimestamp: %v@%v)",
            FormatTransactionId(transactionId, externalizationToken),
            options.CommitTimestamp,
            options.CommitTimestampClusterTag);

        FinishTransaction(transaction);

        ForwardTransactionIfExternalized(
            transaction,
            NProto::TReqCommitExternalizedTransaction{},
            options);

        if (transaction->IsCoarseSerializationNeeded()) {
            auto heapTag = GetSerializingTransactionHeapTag(transaction);
            auto& heap = SerializingTransactionHeaps_[heapTag];
            heap.push_back(transaction);
            AdjustHeapBack(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
            UpdateMinCommitTimestamp(heap);
        } else if (!transaction->IsPerRowSerializationNeeded()) {
            YT_LOG_DEBUG("Transaction removed without serialization (TransactionId: %v)",
                transactionId);

            transaction->SetFinished();

            if (externalizationToken) {
                ExternalizedTransactionMap_.Remove({transactionId, externalizationToken});
            } else {
                PersistentTransactionMap_.Remove(transactionId);
            }
        }
    }

    void AbortTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override
    {
        AbortTransaction(transactionId, {}, options);
    }

    void AbortTransaction(
        TTransactionId transactionId,
        TGuid externalizationToken,
        const NTransactionSupervisor::TTransactionAbortOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId, externalizationToken);

        // Make a copy, transaction may die.
        auto identity = transaction->AuthenticationIdentity();
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto state = transaction->GetPersistentState();
        auto needForce =
            state == ETransactionState::PersistentCommitPrepared ||
            state == ETransactionState::CommitPending;
        if (needForce && !options.Force) {
            transaction->ThrowInvalidState();
        }

        if (IsLeader() && !externalizationToken) {
            CloseLease(transaction);
        }

        transaction->SetPersistentState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        if (transaction->GetTransient()) {
            YT_LOG_ALERT_UNLESS(transaction->Actions().empty(),
                "Transient transaction has actions during abort "
                "(TransactionId: %v, ActionCount: %v)",
                transaction->GetId(),
                transaction->Actions().size());
        } else {
            RunAbortTransactionActions(transaction, options);
        }

        YT_LOG_DEBUG(
            "Transaction aborted (TransactionId: %v, Force: %v, Transient: %v)",
            FormatTransactionId(transactionId, externalizationToken),
            options.Force,
            transaction->GetTransient());

        FinishTransaction(transaction);

        transaction->SetFinished();

        ForwardTransactionIfExternalized(
            transaction,
            NProto::TReqAbortExternalizedTransaction{},
            options);

        if (externalizationToken) {
            ExternalizedTransactionMap_.Remove({transactionId, externalizationToken});
        } else if (transaction->GetTransient()) {
            TransientTransactionMap_.Remove(transactionId);
        } else {
            PersistentTransactionMap_.Remove(transactionId);
        }
    }

    TFuture<void> PingTransaction(TTransactionId transactionId, bool pingAncestors) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

    bool CommitTransaction(TCtxCommitTransactionPtr /*context*/) override
    {
        return false;
    }

    bool AbortTransaction(TCtxAbortTransactionPtr /*context*/) override
    {
        return false;
    }

    void IncrementCommitSignature(TTransaction* transaction, TTransactionSignature delta) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        transaction->CommitSignature() += delta;
        if (transaction->GetPersistentState() == ETransactionState::CommitPending &&
            transaction->CommitSignature() == FinalTransactionSignature)
        {
            const auto& commitOptions = transaction->CommitOptions();
            YT_LOG_DEBUG(
                "Transaction commit signature is completed; committing transaction "
                "(TransactionId: %v, CommitTimestamp: %v@%v)",
                transaction->GetId(),
                commitOptions.CommitTimestamp,
                commitOptions.CommitTimestampClusterTag);

            // NB: May destroy transaction.
            DoCommitTransaction(transaction, transaction->CommitOptions());
        }
    }

    TTimestamp GetMinPrepareTimestamp() const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return PreparedTransactions_.empty()
            ? Host_->GetLatestTimestamp()
            : PreparedTransactions_.begin()->first;
    }

    TTimestamp GetMinCommitTimestamp() const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return MinCommitTimestamp_.value_or(Host_->GetLatestTimestamp());
    }

    void SetDecommission(bool decommission) override
    {
        YT_VERIFY(HasHydraContext());

        if (decommission == Decommission_) {
            return;
        }

        if (decommission) {
            YT_LOG_INFO("Decommission transaction manager");
        } else {
            YT_LOG_INFO("Transaction manager is no longer decommissioned");
        }

        Decommission_ = decommission;
    }

    bool GetDecommission() const override
    {
        return Decommission_;
    }

    void SetRemoving() override
    {
        YT_VERIFY(HasHydraContext());

        YT_LOG_INFO("Transaction manager observes tablet cell removal");

        Removing_ = true;
    }

    bool IsDecommissioned() const override
    {
        return Decommission_ && PersistentTransactionMap_.empty();
    }

    void RegisterTransactionActionHandlers(
        TTransactionActionDescriptor<TTransaction> descriptor) override
    {
        TTransactionManagerBase<TTransaction>::DoRegisterTransactionActionHandlers(
            std::move(descriptor));
    }

private:
    const ITransactionManagerHostPtr Host_;
    const TTransactionManagerConfigPtr Config_;
    const ITransactionLeaseTrackerPtr LeaseTracker_;
    const TCellTag NativeCellTag_;
    const NNative::IConnectionPtr NativeConnection_;
    const TClusterTag ClockClusterTag_;

    NProfiling::TEventTimer TransactionSerializationLagTimer_;

    TEntityMap<TTransaction> PersistentTransactionMap_;
    TEntityMap<TTransaction> TransientTransactionMap_;
    TEntityMap<TExternalizedTransaction> ExternalizedTransactionMap_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    NConcurrency::TPeriodicExecutorPtr BarrierCheckExecutor_;

    THashMap<TCellTag, std::vector<TTransaction*>> SerializingTransactionHeaps_;
    THashMap<TCellTag, TTimestamp> LastSerializedCommitTimestamps_;
    TTimestamp TransientBarrierTimestamp_ = MinTimestamp;

    // Minimal commit timestamp among all committed transactions waiting for serialization.
    std::optional<TTimestamp> MinCommitTimestamp_;

    bool Decommission_ = false;
    bool Removing_ = false;

    IYPathServicePtr OrchidService_;

    std::set<std::pair<TTimestamp, TTransaction*>> PreparedTransactions_;

    TTransactionIdPool AbortTransactionIdPool_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto dumpTransaction = [&] (TFluentMap fluent, const std::pair<TTransactionId, TTransaction*>& pair) {
            auto* transaction = pair.second;
            fluent
                .Item(ToString(transaction->GetId())).BeginMap()
                    .Item("transient").Value(transaction->GetTransient())
                    .Item("timeout").Value(transaction->GetTimeout())
                    .Item("state").Value(transaction->GetTransientState())
                    .Item("start_timestamp").Value(transaction->GetStartTimestamp())
                    .Item("prepare_timestamp").Value(transaction->GetPrepareTimestamp())
                    .Item("left_to_per_row_serialize_part_count").Value(transaction->GetPartsLeftToPerRowSerialize())
                    // Omit CommitTimestamp, it's typically null.
                    // TODO: Tablets.
                .EndMap();
        };
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoFor(TransientTransactionMap_, dumpTransaction)
                .DoFor(PersistentTransactionMap_, dumpTransaction)
            .EndMap();
    }

    TString FormatTransactionId(TTransactionId transactionId, TGuid externalizationToken)
    {
        if (externalizationToken) {
            return Format("%v@%v", transactionId, externalizationToken);
        } else {
            return ToString(transactionId);
        }
    }

    void CreateLease(TTransaction* transaction)
    {
        if (transaction->GetHasLease()) {
            return;
        }

        if (transaction->IsExternalizedToThisCell()) {
            return;
        }

        auto invoker = Host_->GetEpochAutomatonInvoker();

        LeaseTracker_->RegisterTransaction(
            transaction->GetId(),
            NullTransactionId,
            transaction->GetTimeout(),
            /*deadline*/ std::nullopt,
            BIND(&TTransactionManager::OnTransactionExpired, MakeStrong(this))
                .Via(invoker));
        transaction->SetHasLease(true);
    }

    void CloseLease(TTransaction* transaction)
    {
        if (!transaction->GetHasLease()) {
            return;
        }

        if (transaction->IsExternalizedToThisCell()) {
            return;
        }

        LeaseTracker_->UnregisterTransaction(transaction->GetId());
        transaction->SetHasLease(false);
    }


    void OnTransactionExpired(TTransactionId id)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (!transaction) {
            return;
        }

        YT_VERIFY(!transaction->IsExternalizedToThisCell());

        if (transaction->GetTransientState() != ETransactionState::Active) {
            return;
        }

        const auto& transactionSupervisor = Host_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                        id);
                }
            }));
    }

    void FinishTransaction(TTransaction* transaction)
    {
        UnregisterPrepareTimestamp(transaction);
    }

    void OnAfterSnapshotLoaded() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnAfterSnapshotLoaded();

        SerializingTransactionHeaps_.clear();

        for (auto [transactionId, transaction] : PersistentTransactionMap_) {
            auto state = transaction->GetPersistentState();
            YT_VERIFY(transaction->GetTransientState() == state);
            YT_VERIFY(state != ETransactionState::Aborted);
            if (state == ETransactionState::Committed && transaction->IsCoarseSerializationNeeded()) {
                auto heapTag = GetSerializingTransactionHeapTag(transaction);
                SerializingTransactionHeaps_[heapTag].push_back(transaction);
            }
            if (state == ETransactionState::PersistentCommitPrepared ||
                state == ETransactionState::CommitPending)
            {
                RegisterPrepareTimestamp(transaction);
            }
        }

        for (auto& [_, heap] : SerializingTransactionHeaps_) {
            MakeHeap(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
            UpdateMinCommitTimestamp(heap);
        }
    }

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnLeaderActive();

        YT_VERIFY(TransientTransactionMap_.GetSize() == 0);

        // Recreate leases for all active transactions.
        for (auto [transactionId, transaction] : PersistentTransactionMap_) {
            auto state = transaction->GetPersistentState();
            if (state == ETransactionState::Active ||
                state == ETransactionState::PersistentCommitPrepared)
            {
                CreateLease(transaction);
            }
        }

        TransientBarrierTimestamp_ = MinTimestamp;

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Host_->GetEpochAutomatonInvoker(),
            BIND(&TTransactionManager::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();

        BarrierCheckExecutor_ = New<TPeriodicExecutor>(
            Host_->GetEpochAutomatonInvoker(),
            BIND(&TTransactionManager::OnPeriodicBarrierCheck, MakeWeak(this)),
            Config_->BarrierCheckPeriod);
        BarrierCheckExecutor_->Start();

        LeaseTracker_->Start();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnStopLeading();

        if (ProfilingExecutor_) {
            YT_UNUSED_FUTURE(ProfilingExecutor_->Stop());
            ProfilingExecutor_.Reset();
        }

        if (BarrierCheckExecutor_) {
            YT_UNUSED_FUTURE(BarrierCheckExecutor_->Stop());
            BarrierCheckExecutor_.Reset();
        }

        // Drop all transient transactions.
        for (auto [transactionId, transaction] : TransientTransactionMap_) {
            transaction->ResetFinished();
            TransactionTransientReset_.Fire(transaction);
            UnregisterPrepareTimestamp(transaction);
        }
        TransientTransactionMap_.Clear();

        LeaseTracker_->Stop();

        // Reset all transiently prepared persistent transactions back into active state.
        // Mark all transactions as finished to release pending readers.
        // Clear all lease flags.
        for (auto [transactionId, transaction] : PersistentTransactionMap_) {
            if (transaction->GetTransientState() == ETransactionState::TransientCommitPrepared) {
                UnregisterPrepareTimestamp(transaction);
                transaction->SetPrepareTimestamp(NullTimestamp);
            }

            transaction->ResetTransientState();
            transaction->TransientPrepareSignature() = transaction->PersistentPrepareSignature();
            transaction->SetTransientGeneration(transaction->GetPersistentGeneration());
            transaction->ResetFinished();
            transaction->SetHasLease(false);
            TransactionTransientReset_.Fire(transaction);
        }
    }


    void SaveKeys(TSaveContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        PersistentTransactionMap_.SaveKeys(context);
        ExternalizedTransactionMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        PersistentTransactionMap_.SaveValues(context);
        ExternalizedTransactionMap_.SaveValues(context);
        Save(context, LastSerializedCommitTimestamps_);
        Save(context, Decommission_);
        Save(context, Removing_);
    }

    void LoadKeys(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        PersistentTransactionMap_.LoadKeys(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= ETabletReign::SmoothMovementForwardWrites) {
            ExternalizedTransactionMap_.LoadKeys(context);
        }
    }

    void LoadValues(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        PersistentTransactionMap_.LoadValues(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= ETabletReign::SmoothMovementForwardWrites) {
            ExternalizedTransactionMap_.LoadValues(context);
        }
        Load(context, LastSerializedCommitTimestamps_);
        Load(context, Decommission_);
        Load(context, Removing_);
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::Clear();

        TransientTransactionMap_.Clear();
        PersistentTransactionMap_.Clear();
        ExternalizedTransactionMap_.Clear();
        SerializingTransactionHeaps_.clear();
        PreparedTransactions_.clear();
        LastSerializedCommitTimestamps_.clear();
        MinCommitTimestamp_.reset();
        Decommission_ = false;
        Removing_ = false;
    }


    template <class TRequest, class TOptions = std::monostate>
    void ForwardTransactionIfExternalized(
        TTransaction* transaction,
        TRequest request,
        const TOptions& options)
    {
        if (!transaction->IsExternalizedFromThisCell()) {
            return;
        }

        YT_VERIFY(!transaction->IsExternalizedToThisCell());

        for (auto [tabletId, token] : transaction->ExternalizerTablets()) {
            ForwardTransactionIfExternalized(transaction, tabletId, token, request, options);
        }
    }

    template <class TRequest, class TOptions = std::monostate>
    void ForwardTransactionIfExternalized(
        TTransaction* transaction,
        TTabletId tabletId,
        TTransactionExternalizationToken token,
        TRequest request,
        const TOptions& options)
    {
        EObjectType newType;

        switch (TypeFromId(transaction->GetId())) {
            case EObjectType::AtomicTabletTransaction:
                newType = EObjectType::ExternalizedAtomicTabletTransaction;
                break;

            case EObjectType::NonAtomicTabletTransaction:
                YT_ABORT();

            case EObjectType::Transaction:
            case EObjectType::SystemTransaction:
                newType = EObjectType::ExternalizedSystemTabletTransaction;
                break;

            default:
                YT_LOG_FATAL("Attempted to externalize tablet transaction of unknown type "
                    "(TransactionId: %v, Type: %v)",
                    transaction->GetId(),
                    TypeFromId(transaction->GetId()));
                return;
        }

        ToProto(request.mutable_transaction_id(), ReplaceTypeInId(transaction->GetId(), newType));

        // Tablet id is an externalization token for itself.
        ToProto(request.mutable_externalization_token(), token);

        if constexpr (!std::is_same_v<TOptions, std::monostate>) {
            ToProto(request.mutable_options(), options);
        }

        WriteAuthenticationIdentityToProto(
            &request,
            GetCurrentAuthenticationIdentity());

        MutationForwarder_->MaybeForwardMutationToSiblingServant(
            tabletId,
            request);
    }

    void HydraRegisterTransactionActions(NTabletClient::NProto::TReqRegisterTransactionActions* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto externalizationToken = FromProto<TGuid>(request->externalization_token());

        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto* transaction = GetOrCreateTransactionOrThrow(
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            /*transient*/ false,
            externalizationToken);

        if (transaction->GetTransient()) {
            transaction = MakeTransactionPersistentOrThrow(transactionId);
        }

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);

            YT_LOG_DEBUG("Transaction action registered (TransactionId: %v, ActionType: %v, Signature: %v)",
                FormatTransactionId(transactionId, externalizationToken),
                data.Type,
                signature);
        }

        ForwardTransactionIfExternalized(transaction, *request, /*options*/ {});

        transaction->PersistentPrepareSignature() += signature;
        // NB: May destroy transaction.
        IncrementCommitSignature(transaction, signature);
    }

    void HydraHandleTransactionBarrier(NTabletNode::NProto::TReqHandleTransactionBarrier* request)
    {
        auto barrierTimestamp = request->timestamp();

        YT_LOG_DEBUG("Handling transaction barrier (Timestamp: %v)",
            barrierTimestamp);

        for (auto& [_, heap]: SerializingTransactionHeaps_) {
            while (!heap.empty()) {
                auto* transaction = heap.front();
                auto commitTimestamp = transaction->GetCommitTimestamp();
                if (commitTimestamp > barrierTimestamp) {
                    break;
                }

                UpdateLastSerializedCommitTimestamp(transaction);

                auto transactionId = transaction->GetId();
                YT_LOG_DEBUG("Transaction serialized (TransactionId: %v, CommitTimestamp: %v)",
                    transaction->GetId(),
                    commitTimestamp);

                transaction->SetPersistentState(ETransactionState::Serialized);
                BeforeTransactionCoarselySerialized_.Fire(transaction);
                TransactionCoarselySerialized_.Fire(transaction);

                // NB: Update replication progress after all rows are serialized and available for pulling.
                RunSerializeTransactionActions(transaction);

                if (transaction->CoarseSerializingTabletIds().empty() && transaction->PerRowSerializingTabletIds().empty()) {
                    transaction->SetFinished();

                    PersistentTransactionMap_.Remove(transactionId);
                }

                ExtractHeap(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
                heap.pop_back();
            }
        }

        MinCommitTimestamp_.reset();
        for (const auto& heap : SerializingTransactionHeaps_) {
            UpdateMinCommitTimestamp(heap.second);
        }

        YT_LOG_DEBUG("Min commit timestamp was updated (MinCommitTimestamp: %v)",
            MinCommitTimestamp_);

        // YT-8542: It is important to update this timestamp only _after_ all relevant transactions are serialized.
        // See TTableReplicator.
        // Note that runtime data may be missing in unittests.
        if (const auto& runtimeData = Host_->GetRuntimeData()) {
            runtimeData->BarrierTimestamp.store(barrierTimestamp);
        }

        TransactionBarrierHandled_.Fire(barrierTimestamp);
    }

    void HydraExternalizeTransaction(NProto::TReqExternalizeTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto tabletId = FromProto<TTabletId>(request->externalizer_tablet_id());
        auto token = FromProto<TTransactionExternalizationToken>(
            request->externalization_token());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto* transaction = GetOrCreateTransactionOrThrow(
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            /*transient*/ false);

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        EmplaceOrCrash(
            transaction->ExternalizerTablets(),
            tabletId,
            token);

        YT_LOG_DEBUG("Transaction externalized "
            "(TabletId: %v, TransactionId: %v, ExternalizationToken: %v)",
            tabletId,
            transactionId,
            token);
    }

    void HydraPrepareExternalizedTransaction(NProto::TReqPrepareExternalizedTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto externalizationToken = FromProto<TGuid>(request->externalization_token());
        auto options = FromProto<TTransactionPrepareOptions>(request->options());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        YT_LOG_DEBUG("Preparing externalized transaction "
            "(TransactionId: %v, PrepareTimestamp: %v)",
            FormatTransactionId(transactionId, externalizationToken),
            options.PrepareTimestamp);

        YT_VERIFY(options.Persistent);

        try {
            PrepareTransactionCommit(
                transactionId,
                externalizationToken,
                options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to prepare externalized transaction (TransactionId: %v)",
                FormatTransactionId(transactionId, externalizationToken));
        }
    }

    void HydraCommitExternalizedTransaction(NProto::TReqCommitExternalizedTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto externalizationToken = FromProto<TGuid>(request->externalization_token());
        auto options = FromProto<TTransactionCommitOptions>(request->options());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        YT_LOG_DEBUG("Committing externalized transaction (TransactionId: %v)",
            FormatTransactionId(transactionId, externalizationToken));

        try {
            CommitTransaction(transactionId, externalizationToken, options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to commit externalized transaction (TransactionId: %v)",
                FormatTransactionId(transactionId, externalizationToken));

            throw;
        }
    }

    void HydraAbortExternalizedTransaction(NProto::TReqAbortExternalizedTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto externalizationToken = FromProto<TGuid>(request->externalization_token());
        auto options = FromProto<TTransactionAbortOptions>(request->options());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        YT_LOG_DEBUG("Aborting externalized transaction (TransactionId: %v)",
            FormatTransactionId(transactionId, externalizationToken));

        AbortTransaction(transactionId, externalizationToken, options);
    }

    TDuration ComputeTransactionSerializationLag() const
    {
        if (PreparedTransactions_.empty()) {
            return TDuration::Zero();
        }

        auto latestTimestamp = Host_->GetLatestTimestamp();
        auto minPrepareTimestamp = PreparedTransactions_.begin()->first;
        if (minPrepareTimestamp > latestTimestamp) {
            return TDuration::Zero();
        }

        return TimestampDiffToDuration(minPrepareTimestamp, latestTimestamp).second;
    }


    void OnProfiling()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TransactionSerializationLagTimer_.Record(ComputeTransactionSerializationLag());
    }


    void OnPeriodicBarrierCheck()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_DEBUG("Running periodic barrier check (BarrierTimestamp: %v, MinPrepareTimestamp: %v)",
            TransientBarrierTimestamp_,
            GetMinPrepareTimestamp());

        CheckBarrier();
    }

    void CheckBarrier()
    {
        if (!IsActiveLeader()) {
            return;
        }

        auto minPrepareTimestamp = GetMinPrepareTimestamp();
        if (minPrepareTimestamp <= TransientBarrierTimestamp_) {
            return;
        }

        NTracing::TNullTraceContextGuard guard;

        YT_LOG_DEBUG("Committing transaction barrier (Timestamp: %v -> %v)",
            TransientBarrierTimestamp_,
            minPrepareTimestamp);

        TransientBarrierTimestamp_ = minPrepareTimestamp;

        NTabletNode::NProto::TReqHandleTransactionBarrier request;
        request.set_timestamp(TransientBarrierTimestamp_);
        YT_UNUSED_FUTURE(CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger));
    }

    void RegisterPrepareTimestamp(TTransaction* transaction)
    {
        auto prepareTimestamp = transaction->GetPrepareTimestamp();
        if (prepareTimestamp == NullTimestamp) {
            return;
        }
        YT_VERIFY(PreparedTransactions_.emplace(prepareTimestamp, transaction).second);
    }

    void UnregisterPrepareTimestamp(TTransaction* transaction)
    {
        auto prepareTimestamp = transaction->GetPrepareTimestamp();
        if (prepareTimestamp == NullTimestamp) {
            return;
        }
        auto pair = std::pair(prepareTimestamp, transaction);
        auto it = PreparedTransactions_.find(pair);
        YT_VERIFY(it != PreparedTransactions_.end());
        PreparedTransactions_.erase(it);
        CheckBarrier();
    }

    void UpdateLastSerializedCommitTimestamp(TTransaction* transaction)
    {
        auto commitTimestamp = transaction->GetCommitTimestamp();
        auto cellTag = transaction->GetCellTag();

        if (auto lastTimestampIt = LastSerializedCommitTimestamps_.find(cellTag)) {
            if (commitTimestamp <= lastTimestampIt->second) {
                // TODO(ponasenko-rs): Remove condition after YT-20361.
                YT_LOG_ALERT_IF(transaction->GetPrepareTimestamp() != NullTimestamp,
                    "The clock has gone back (CellTag: %v, LastSerializedCommitTimestamp: %v, CommitTimestamp: %v)",
                    cellTag,
                    lastTimestampIt->second,
                    commitTimestamp);
                return;
            }

            lastTimestampIt->second = commitTimestamp;
        } else {
            YT_VERIFY(LastSerializedCommitTimestamps_.emplace(cellTag, commitTimestamp).second);
        }
    }

    void UpdateMinCommitTimestamp(const std::vector<TTransaction*>& heap)
    {
        if (heap.empty()) {
            return;
        }

        auto timestamp = heap.front()->GetCommitTimestamp();
        MinCommitTimestamp_ = std::min(timestamp, MinCommitTimestamp_.value_or(timestamp));
    }

    void ValidateNotDecommissioned(TTransaction* transaction)
    {
        if (!Decommission_) {
            return;
        }

        if (Removing_ &&
            TypeFromId(transaction->GetId()) == EObjectType::Transaction &&
            transaction->AuthenticationIdentity() == GetRootAuthenticationIdentity())
        {
            YT_LOG_ALERT("Allow transaction in decommissioned state to proceed "
                "(TransactionId: %v, AuthenticationIdentity: %v)",
                transaction->GetId(),
                transaction->AuthenticationIdentity());
            return;
        }

        THROW_ERROR_EXCEPTION("Tablet cell is decommissioned");
    }

    void ValidateTimestampClusterTag(
        TTransactionId transactionId,
        TClusterTag timestampClusterTag,
        TTimestamp prepareTimestamp,
        bool canThrow)
    {
        if (prepareTimestamp == NullTimestamp) {
            return;
        }

        if (ClockClusterTag_ == InvalidCellTag || timestampClusterTag == InvalidCellTag) {
            return;
        }

        if (ClockClusterTag_ != timestampClusterTag) {
            if (Config_->RejectIncorrectClockClusterTag && canThrow) {
                THROW_ERROR_EXCEPTION("Transaction timestamp is generated from unexpected clock")
                    << TErrorAttribute("transaction_id", transactionId)
                    << TErrorAttribute("timestamp_cluster_tag", timestampClusterTag)
                    << TErrorAttribute("clock_cluster_tag", ClockClusterTag_);
            }

            YT_LOG_ALERT("Transaction timestamp is generated from unexpected clock (TransactionId: %v, TransactionClusterTag: %v, ClockClusterTag: %v)",
                transactionId,
                timestampClusterTag,
                ClockClusterTag_);
        }
    }

    TCellTag GetSerializingTransactionHeapTag(TTransaction* transaction)
    {
        return transaction->GetCommitTimestampClusterTag() != InvalidCellTag
            ? transaction->GetCommitTimestampClusterTag()
            : transaction->GetCellTag();
    }

    static bool SerializingTransactionHeapComparer(
        const TTransaction* lhs,
        const TTransaction* rhs)
    {
        YT_ASSERT(lhs->GetPersistentState() == ETransactionState::Committed);
        YT_ASSERT(rhs->GetPersistentState() == ETransactionState::Committed);
        return lhs->GetCommitTimestamp() < rhs->GetCommitTimestamp();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(
    TTransactionManagerConfigPtr config,
    ITransactionManagerHostPtr host,
    TClusterTag clockClusterTag,
    ITransactionLeaseTrackerPtr transactionLeaseTracker)
{
    return New<TTransactionManager>(
        std::move(config),
        std::move(host),
        clockClusterTag,
        std::move(transactionLeaseTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
