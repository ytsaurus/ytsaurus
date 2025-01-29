#include "transaction_manager.h"

#include "private.h"

#include "config.h"
#include "boomerang_tracker.h"
#include "sequoia_integration.h"
#include "transaction_presence_cache.h"
#include "transaction_replication_session.h"
#include "transaction.h"
#include "transaction_type_handler.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>
#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>
#include <yt/yt/server/lib/lease_server/proto/lease_manager.pb.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>
#include <yt/yt/server/lib/transaction_server/private.h>

#include <yt/yt/server/lib/transaction_supervisor/config.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_manager_detail.h>

#include <yt/yt/server/lib/transaction_supervisor/proto/transaction_supervisor.pb.h>

#include <yt/yt/server/master/object_server/attribute_set.h>
#include <yt/yt/server/master/object_server/object.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/sequoia_server/context.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/sequoia/protobuf_helpers.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/transaction_client/proto/transaction_service.pb.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/id_generator.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <library/cpp/yt/compact_containers/compact_queue.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NElection;
using namespace NHydra;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NLeaseServer;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NTransactionClient::NProto;
using namespace NSecurityServer;
using namespace NProfiling;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTransactionSupervisor;

using NTransactionSupervisor::NProto::NTransactionSupervisor::TRspCommitTransaction;
using NTransactionSupervisor::NProto::NTransactionSupervisor::TRspAbortTransaction;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TMasterAutomatonPart
    , public ITransactionManager
    , public TTransactionManagerBase<TTransaction>
{
public:
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DEFINE_SIGNAL_OVERRIDE(void(TTransaction*), TransactionAborted);

    DEFINE_BYREF_RO_PROPERTY(TTransactionPresenceCachePtr, TransactionPresenceCache);

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Transaction, TTransaction);

public:
    explicit TTransactionManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TransactionManager)
        , TransactionPresenceCache_(New<TTransactionPresenceCache>(Bootstrap_))
        , BoomerangTracker_(New<TBoomerangTracker>(Bootstrap_))
        , BufferedProducer_(New<TBufferedProducer>())
        , LeaseTracker_(CreateTransactionLeaseTracker(
            Bootstrap_->GetTransactionLeaseTrackerThreadPool(),
            TransactionServerLogger()))
    {
        TransactionServerProfiler().AddProducer("", BufferedProducer_);

        YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

        Logger = TransactionServerLogger();

        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraStartTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraStartCypressTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraStartForeignTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraRegisterTransactionActions, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraPrepareTransactionCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraCommitTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraAbortTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraCommitCypressTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraAbortCypressTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraReplicateTransactions, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraNoteNoSuchTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraReturnBoomerang, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraRemoveStuckBoomerangWaves, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraIssueLeases, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraRevokeLeases, Unretained(this)));

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
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TTransactionManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::Transaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::NestedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::ExternalizedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::ExternalizedNestedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::UploadTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::UploadNestedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::SystemTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::SystemNestedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(Bootstrap_, EObjectType::AtomicTabletTransaction));

        const auto& leaseManager = Bootstrap_->GetLeaseManager();
        leaseManager->SubscribeLeaseRevoked(BIND_NO_PROPAGATE(&TTransactionManager::OnLeaseRevoked, MakeWeak(this)));

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TTransactionManager::OnProfiling, MakeWeak(this)),
            TDynamicTransactionManagerConfig::DefaultProfilingPeriod);
        ProfilingExecutor_->Start();

        // Start Cypress Tx
        // Coordinator: TReqStartCypressTransaction, late prepare
        // Participant: TReqStartForeignTransaction, commit only
        RegisterTransactionActionHandlers<NProto::TReqStartCypressTransaction>({
            .Prepare = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraPrepareAndCommitStartCypressTransactionInSequoia,
                Unretained(this)),
        });
        RegisterTransactionActionHandlers<NProto::TReqStartForeignTransaction>({
            .Commit = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraCommitStartForeignTransactionInSequoia,
                Unretained(this)),
        });

        // Commit Cypress Tx
        // Coordinator: TReqCommitCypressTransaction, late prepare
        // Participant: TReqCommitTransaction, commit only
        RegisterTransactionActionHandlers<NProto::TReqCommitCypressTransaction>({
            .Prepare = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraPrepareAndCommitCommitCypressTransactionInSequoia,
                Unretained(this)),
        });
        // TODO(kvk1920): consider renaming TReqCommitTransaction ->
        // TReqCommitForeignTransaction.
        RegisterTransactionActionHandlers<NProto::TReqCommitTransaction>({
            .Commit = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraCommitCommitForeignTransactionInSequoia,
                Unretained(this)),
        });

        // Abort Cypress Tx
        // Coordinator: TReqAbortCypressTransaction, late prepare
        // Participant: TReqAbortTransaction, commit only
        RegisterTransactionActionHandlers<NProto::TReqAbortCypressTransaction>({
            .Prepare = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraPrepareAndCommitAbortCypressTransactionInSequoia,
                Unretained(this)),
        });
        RegisterTransactionActionHandlers<NProto::TReqAbortTransaction>({
            .Commit = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraCommitAbortForeignTransactionInSequoia,
                Unretained(this)),
        });

        // Replicate Cypress Tx
        // Coordinator: TReqMarkCypressTransactionsReplicatedToCell, prepare
        // Participant: TReqMaterializeCypressTransactionReplicas, prepare
        // NB: Since there is no permission checking nor any validation on tx
        // replication we could do everything in tx action's commit.
        RegisterTransactionActionHandlers<NProto::TReqMarkCypressTransactionsReplicatedToCells>({
            .Commit = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraCommitMarkCypressTransactionsReplicatedToCell,
                Unretained(this)),
        });
        RegisterTransactionActionHandlers<NProto::TReqMaterializeCypressTransactionReplicas>({
            .Commit = BIND_NO_PROPAGATE(
                &TTransactionManager::HydraCommitMaterializeCypressTransactionReplicas,
                Unretained(this)),
        });
    }

    const TTransactionPresenceCachePtr& GetTransactionPresenceCache() override
    {
        return TransactionPresenceCache_;
    }

    TTransaction* StartSystemTransaction(
        const TCellTagList& replicatedToCellTags,
        std::optional<TDuration> timeout,
        const std::string& title,
        const IAttributeDictionary& attributes,
        TTransactionId hintId = NullTransactionId) override
    {
        return StartTransaction(
            /*parent*/ nullptr,
            /*prerequisiteTransactions*/ {},
            replicatedToCellTags,
            /*timeout*/ timeout,
            /*deadline*/ std::nullopt,
            title,
            attributes,
            /*isCypressTransaction*/ false,
            hintId);
    }

    TTransaction* StartNonMirroredCypressTransaction(
        const TCellTagList& replicatedToCellTags,
        const std::string& title) override
    {
        return StartTransaction(
            /*parent*/ nullptr,
            /*prerequisiteTransactions*/ {},
            replicatedToCellTags,
            /*timeout*/ std::nullopt,
            /*deadline*/ std::nullopt,
            title,
            EmptyAttributes(),
            /*isCypressTransaction*/ true);
    }


    TTransaction* StartTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const TCellTagList& replicatedToCellTags,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        const std::optional<std::string>& title,
        const IAttributeDictionary& attributes,
        bool isCypressTransaction,
        TTransactionId hintId = NullTransactionId)
    {
        ValidateNativeTransactionStart(parent, prerequisiteTransactions);

        return DoStartTransaction(
            /*upload*/ false,
            parent,
            std::move(prerequisiteTransactions),
            replicatedToCellTags,
            timeout,
            deadline,
            title,
            attributes,
            isCypressTransaction,
            /*enableNativeTxExternalization*/ std::nullopt,
            hintId);
    }

    TTransaction* StartUploadTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const TCellTagList& replicatedToCellTags,
        std::optional<TDuration> timeout,
        const std::optional<std::string>& title,
        TTransactionId hintId) override
    {
        ValidateUploadTransactionStart(parent);

        return DoStartTransaction(
            /*upload*/ true,
            parent,
            prerequisiteTransactions,
            replicatedToCellTags,
            timeout,
            /*deadline*/ std::nullopt,
            title,
            EmptyAttributes(),
            /*isCypressTransaction*/ false,
            /*enableNativeTxExternalization*/ std::nullopt,
            hintId);
    }

    void ValidateGenericTransactionStart(TTransaction* parent)
    {
        if (!parent) {
            return;
        }

        if (parent->IsUpload()) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::UploadTransactionCannotHaveNested,
                "Failed to start a transaction nested in an upload transaction")
                << TErrorAttribute("upload_transaction_id", parent->GetId());
        }
    }

    void ValidateNativeTransactionStart(
        TTransaction* parent,
        const std::vector<TTransaction*>& prerequisiteTransactions)
    {
        ValidateGenericTransactionStart(parent);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto thisCellTag = multicellManager->GetCellTag();

        if (parent && CellTagFromId(parent->GetId()) != thisCellTag) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::ForeignParentTransaction,
                "Parent transaction is foreign")
                << TErrorAttribute("parent_transaction_id", parent->GetId())
                << TErrorAttribute("parent_transaction_cell_tag", CellTagFromId(parent->GetId()))
                << TErrorAttribute("expected_cell_tag", thisCellTag);
        }

        for (auto* prerequisiteTransaction : prerequisiteTransactions) {
            if (CellTagFromId(prerequisiteTransaction->GetId()) != thisCellTag) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::ForeignPrerequisiteTransaction,
                    "Prerequisite transaction is foreign")
                    << TErrorAttribute("prerequisite_transaction_id", prerequisiteTransaction->GetId())
                    << TErrorAttribute("prerequisite_transaction_cell_tag", CellTagFromId(prerequisiteTransaction->GetId()))
                    << TErrorAttribute("expected_cell_tag", thisCellTag);
            }
        }
    }

    void ValidateUploadTransactionStart(TTransaction* parent)
    {
        ValidateGenericTransactionStart(parent);
    }

    TTransaction* DoStartTransaction(
        bool upload,
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        TCellTagList replicatedToCellTags,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        const std::optional<std::string>& title,
        const IAttributeDictionary& attributes,
        bool isCypressTransaction,
        std::optional<bool> enableNativeTxExternalization,
        TTransactionId hintId)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        const auto& dynamicConfig = GetDynamicConfig();

        EObjectType transactionObjectType;
        if (upload) {
            transactionObjectType = parent
                ? EObjectType::UploadNestedTransaction
                : EObjectType::UploadTransaction;
        } else if (!isCypressTransaction) {
            transactionObjectType = parent
                ? EObjectType::SystemNestedTransaction
                : EObjectType::SystemTransaction;
        } else {
            transactionObjectType = parent
                ? EObjectType::NestedTransaction
                : EObjectType::Transaction;
        }

        // COMPAT(h0pless): Replace this with ThrowErrorException when CTxS will be used by all clients.
        // NB: Upload transaction can be nested to both system and Cypress transaction.
        if (parent && !IsUploadTransactionType(transactionObjectType)) {
            auto parentType = TypeFromId(parent->GetId());

            if (IsSystemTransactionType(transactionObjectType) && !IsSystemTransactionType(parentType)) {
                YT_LOG_ALERT("An attempt to create a system transaction nested inside of non-system parent was made "
                    "(ParentId: %v, ParentType: %v, RequestedChildType: %v, HintId: %v)",
                    parent->GetId(),
                    parentType,
                    transactionObjectType,
                    hintId);
                transactionObjectType = EObjectType::NestedTransaction;
            }

            if (!IsSystemTransactionType(transactionObjectType) && IsSystemTransactionType(parentType)) {
                YT_LOG_ALERT("An attempt to create a non-system transaction nested inside of system parent was made "
                    "(ParentId: %v, ParentType: %v, RequestedChildType: %v, HintId: %v)",
                    parent->GetId(),
                    parentType,
                    transactionObjectType,
                    hintId);
                transactionObjectType = EObjectType::SystemNestedTransaction;
            }
        }

        if (parent) {
            if (parent->GetPersistentState() != ETransactionState::Active) {
                parent->ThrowInvalidState();
            }

            if (parent->GetDepth() >= dynamicConfig->MaxTransactionDepth) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::TransactionDepthLimitReached,
                    "Transaction depth limit reached")
                    << TErrorAttribute("limit", dynamicConfig->MaxTransactionDepth);
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto transactionId = objectManager->GenerateId(transactionObjectType, hintId);

        auto transactionHolder = TPoolAllocator::New<TTransaction>(transactionId, upload);
        auto* transaction = TransactionMap_.Insert(transactionId, std::move(transactionHolder));

        // Every active transaction has a fake reference to itself.
        YT_VERIFY(transaction->RefObject() == 1);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto native = (CellTagFromId(transactionId) == multicellManager->GetCellTag());

        if (parent) {
            transaction->SetParent(parent);
            transaction->SetDepth(parent->GetDepth() + 1);
            YT_VERIFY(parent->NestedTransactions().insert(transaction).second);
            objectManager->RefObject(transaction);
        }

        if (native) {
            InsertOrCrash(NativeTransactions_, transaction);
            if (!parent) {
                InsertOrCrash(NativeTopmostTransactions_, transaction);
            }
        } else {
            InsertOrCrash(ForeignTransactions_, transaction);
        }

        if (IsCypressTransactionType(transaction->GetType())) {
            if (parent) {
                transaction->SetNativeTxExternalizationEnabled(parent->IsNativeTxExternalizationEnabled());
            } else if (native) {
                transaction->SetNativeTxExternalizationEnabled(true);
            } else {
                YT_VERIFY(enableNativeTxExternalization.has_value());

                transaction->SetNativeTxExternalizationEnabled(*enableNativeTxExternalization);
            }
        }

        transaction->SetPersistentState(ETransactionState::Active);
        transaction->PrerequisiteTransactions() = std::move(prerequisiteTransactions);
        for (auto* prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            // NB: Duplicates are fine; prerequisite transactions may be duplicated.
            prerequisiteTransaction->DependentTransactions().insert(transaction);
        }

        if (!native) {
            transaction->SetForeign();
        }

        auto replicated = !native && isCypressTransaction;
        if (!replicated && timeout) {
            transaction->SetTimeout(std::min(*timeout, dynamicConfig->MaxTransactionTimeout));
        }

        if (!replicated) {
            transaction->SetDeadline(deadline);
        }

        transaction->SetIsCypressTransaction(isCypressTransaction);

        if (IsLeader()) {
            CreateLease(transaction);
        }

        transaction->SetTitle(title);

        // NB: This is not quite correct for replicated transactions but we don't care.
        const auto* mutationContext = GetCurrentMutationContext();
        transaction->SetStartTime(mutationContext->GetTimestamp());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        transaction->Acd().SetOwner(user);

        objectManager->FillAttributes(transaction, attributes);

        if (!replicatedToCellTags.empty()) {
            // Never include native cell tag into ReplicatedToCellTags.
            replicatedToCellTags.erase(
                std::remove(
                    replicatedToCellTags.begin(),
                    replicatedToCellTags.end(),
                    CellTagFromId(transactionId)),
                replicatedToCellTags.end());

            if (upload) {
                transaction->ReplicatedToCellTags() = replicatedToCellTags;
            } else if (IsMirroringToSequoiaEnabled() && IsMirroredToSequoia(transactionId)) {
                MarkTransactionReplicated(transaction, replicatedToCellTags);
            } else {
                ReplicateTransaction(transaction, replicatedToCellTags);
            }
        }

        TransactionStarted_.Fire(transaction);

        auto time = timer.GetElapsedTime();

        YT_LOG_ACCESS("StartTransaction", transaction);

        YT_LOG_DEBUG("Transaction started (TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, "
            "ReplicatedToCellTags: %v, Timeout: %v, Deadline: %v, User: %v, Title: %v, WallTime: %v, "
            "MirroredToSequoia: %v, NativeTxExternalizationEnabled: %v)",
            transactionId,
            GetObjectId(parent),
            MakeFormattableView(transaction->PrerequisiteTransactions(), [] (auto* builder, const auto* prerequisiteTransaction) {
                FormatValue(builder, prerequisiteTransaction->GetId(), TStringBuf());
            }),
            replicatedToCellTags,
            transaction->GetTimeout(),
            transaction->GetDeadline(),
            user->GetName(),
            title,
            time,
            IsMirroredToSequoia(transactionId),
            transaction->IsNativeTxExternalizationEnabled());

        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});

        CacheTransactionStarted(transaction);

        return transaction;
    }

    void MarkTransactionReplicated(TTransaction* transaction, TCellTagList replicateToCellTags)
    {
        for (; transaction; transaction = transaction->GetParent()) {
            bool alreadyReplicated = true;
            for (auto cellTag : replicateToCellTags) {
                if (!transaction->IsReplicatedToCell(cellTag)) {
                    alreadyReplicated = false;
                    transaction->ReplicatedToCellTags().push_back(cellTag);
                }
            }
            if (alreadyReplicated) {
                break;
            }
        }
    }

    void CommitMasterTransaction(
        TTransaction* transaction,
        const TTransactionCommitOptions& options) override
    {
        YT_LOG_ALERT_IF(
            transaction->GetIsCypressTransaction() && IsSequoiaId(transaction->GetId()),
            "Attempt to commit Cypress transaction in non-Sequoia way (TransactionId: %v)",
            transaction->GetId());

        CommitTransaction(transaction, options);
    }

    void CommitTransaction(
        TTransaction* transaction,
        const TTransactionCommitOptions& options,
        bool replicateViaHive = true)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        NProfiling::TWallTimer timer;

        YT_VERIFY(transaction->IsForeign() || transaction->GetNativeCommitMutationRevision() == NHydra::NullRevision);

        auto transactionId = transaction->GetId();

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            YT_LOG_DEBUG("Transaction is already committed (TransactionId: %v)",
                transactionId);
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        // This is ensured by PrepareTransactionCommit in the same mutation.
        YT_VERIFY(transaction->GetSuccessorTransactionLeaseCount() == 0);

        // The timestamp from the holder is used by two parties:
        //  - chunk view sets override timestamp when fetched;
        //  - tablet manager sends this timestamp to the node when the tablet is unlocked.
        // If all outputs are empty, there are no chunk views so we have to ref the
        // holder so tablet manager has access to the timestamp.
        // There is a corner case when all outputs are empty and no table is locked
        // (the user may ignore atomicity and explicitly ask not to lock his tables,
        // mostly non-atomic ones). However, in this case the tablet may be safely
        // unlocked with null timestamp.
        bool temporaryRefTimestampHolder = false;
        if (!transaction->LockedDynamicTables().empty()) {
            temporaryRefTimestampHolder = true;
            CreateOrRefTimestampHolder(transactionId);
        }

        SetTimestampHolderTimestamp(transactionId, options.CommitTimestamp);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (replicateViaHive && !transaction->ReplicatedToCellTags().empty()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_commit_timestamp(options.CommitTimestamp);
            const auto* mutationContext = GetCurrentMutationContext();
            request.set_native_commit_mutation_revision(ToProto(mutationContext->GetVersion().ToRevision()));
            multicellManager->PostToMasters(request, transaction->ReplicatedToCellTags());
        }

        if (!transaction->ExternalizedToCellTags().empty()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag()));
            request.set_commit_timestamp(options.CommitTimestamp);
            const auto* mutationContext = GetCurrentMutationContext();
            request.set_native_commit_mutation_revision(ToProto(mutationContext->GetVersion().ToRevision()));
            multicellManager->PostToMasters(request, transaction->ExternalizedToCellTags());
        }

        // Abort of nested transactions has to be done after replicating commit
        // to participants and that's why:
        // abort of nested transactions cause posting "remove object"
        // mutations via Hive. It must be done only after tx abort on foreign
        // cells since txs hold refs to objects and "remove object" checks if
        // object's refcount is zero.
        TCompactVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectIdComparer());
        for (auto* nestedTransaction : nestedTransactions) {
            YT_LOG_DEBUG("Aborting nested transaction on parent commit (TransactionId: %v, ParentId: %v)",
                nestedTransaction->GetId(),
                transactionId);
            TTransactionAbortOptions options{
                .Force = true,
            };
            // NB: Disable replication via Hive as the commit sent above will
            // abort them implicitly.
            AbortTransaction(
                nestedTransaction,
                options,
                /*validatePermissions*/ false,
                /*replicateViaHive*/ false);
        }
        YT_VERIFY(transaction->NestedTransactions().empty());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetPersistentState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        if (temporaryRefTimestampHolder) {
            UnrefTimestampHolder(transactionId);
        }

        auto sequoiaContextGuard = MaybeCreateSequoiaContextGuard(transaction);

        RunCommitTransactionActions(transaction, options);
        if (IsSequoiaTxBarrierEnabled() && transaction->IsSequoiaTransaction()) {
            Bootstrap_->GetTransactionSupervisor()->UnregisterPreparedSequoiaTx(transaction->GetId());
        }

        if (auto* parent = transaction->GetParent()) {
            parent->ExportedObjects().insert(
                parent->ExportedObjects().end(),
                transaction->ExportedObjects().begin(),
                transaction->ExportedObjects().end());
            parent->ImportedObjects().insert(
                parent->ImportedObjects().end(),
                transaction->ImportedObjects().begin(),
                transaction->ImportedObjects().end());

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->RecomputeTransactionAccountResourceUsage(parent);
        } else {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* object : transaction->ImportedObjects()) {
                objectManager->UnrefObject(object);
            }
            // Remove extra reference.
            for (const auto& exportEntry : transaction->ExportedObjects()) {
                objectManager->UnrefObject(exportEntry.Object);
            }
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        auto* user = transaction->Acd().GetOwner()->AsUser();

        YT_LOG_ACCESS("CommitTransaction", transaction);

        FinishTransaction(transaction);

        auto time = timer.GetElapsedTime();

        YT_LOG_DEBUG(
            "Transaction committed (TransactionId: %v, User: %v, CommitTimestamp: %v@%v, WallTime: %v)",
            transactionId,
            user->GetName(),
            options.CommitTimestamp,
            options.CommitTimestampClusterTag,
            time);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});
    }

    void AbortMasterTransaction(
        TTransaction* transaction,
        const TTransactionAbortOptions& options) override
    {
        YT_LOG_ALERT_IF(
            transaction->GetIsCypressTransaction() && IsSequoiaId(transaction->GetId()),
            "Attempt to abort mirrored Cypress transaction in non-Sequoia way (TransactionId: %v)",
            transaction->GetId());

        AbortTransaction(
            transaction,
            options,
            /*validatePermissions*/ false,
            /*replicateViaHive*/ true);
    }

    void AbortTransaction(
        TTransaction* transaction,
        const TTransactionAbortOptions& options,
        bool validatePermissions,
        bool replicateViaHive)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        auto transactionId = transaction->GetId();

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Aborted) {
            return;
        }

        if (state == ETransactionState::PersistentCommitPrepared && !options.Force ||
            state == ETransactionState::Committed)
        {
            transaction->ThrowInvalidState();
        }

        if (transaction->GetSuccessorTransactionLeaseCount() > 0) {
            if (options.Force) {
                RevokeLeases(transaction, /*force*/ true);
                YT_VERIFY(transaction->GetSuccessorTransactionLeaseCount() == 0);
            } else {
                ThrowTransactionSuccessorHasLeases(transaction);
            }
        }

        if (validatePermissions) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(transaction, EPermission::Write);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (replicateViaHive && !transaction->ReplicatedToCellTags().empty()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_force(true);
            multicellManager->PostToMasters(request, transaction->ReplicatedToCellTags());
        }

        if (!transaction->ExternalizedToCellTags().empty()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag()));
            request.set_force(true);
            multicellManager->PostToMasters(request, transaction->ExternalizedToCellTags());
        }

        TCompactVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectIdComparer());
        for (auto* nestedTransaction : nestedTransactions) {
            TTransactionAbortOptions options{
                .Force = true,
            };

            // NB: It's not necessary to replicate every nested transaction's
            // abort since it will be aborted during ancestor's abort anyway.
            AbortTransaction(
                nestedTransaction,
                options,
                /*validatePermissions*/ false,
                /*replicateViaHive*/ false);
        }
        YT_VERIFY(transaction->NestedTransactions().empty());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetPersistentState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        RunAbortTransactionActions(transaction, options);
        if (IsSequoiaTxBarrierEnabled() && transaction->IsSequoiaTransaction()) {
            Bootstrap_->GetTransactionSupervisor()->UnregisterPreparedSequoiaTx(transaction->GetId());
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (const auto& entry : transaction->ExportedObjects()) {
            auto* object = entry.Object;
            objectManager->UnrefObject(object);
            // Remove extra reference.
            objectManager->UnrefObject(object);
            const auto& handler = objectManager->GetHandler(object);
            handler->UnexportObject(object, entry.DestinationCellTag, 1);
        }
        for (auto* object : transaction->ImportedObjects()) {
            objectManager->UnrefObject(object);
            object->ImportUnrefObject();
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        auto* user = transaction->Acd().GetOwner()->AsUser();

        YT_LOG_ACCESS("AbortTransaction", transaction);

        FinishTransaction(transaction);

        auto time = timer.GetElapsedTime();

        YT_LOG_DEBUG("Transaction aborted (TransactionId: %v, User: %v, Force: %v, Title: %v, WallTime: %v)",
            transactionId,
            user->GetName(),
            options.Force,
            transaction->GetTitle(),
            time);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});
    }

    void ReplicateTransaction(TTransaction* transaction, TCellTagList dstCellTags)
    {
        YT_VERIFY(IsObjectAlive(transaction));
        YT_VERIFY(transaction->IsNative());
        auto replicatedTransactionId = ExternalizeOrReplicateTransaction(
            transaction,
            dstCellTags,
            /*shouldExternalize*/ false);

        YT_VERIFY(replicatedTransactionId == transaction->GetId());
    }

    TTransactionId ExternalizeTransaction(
        TTransaction* transaction,
        TCellTagList dstCellTags) override
    {
        if (!transaction) {
            return {};
        }

        bool shouldExternalize = transaction->IsNative()
            ? transaction->IsNativeTxExternalizationEnabled()
            : true;
        return ExternalizeOrReplicateTransaction(transaction, dstCellTags, shouldExternalize);
    }

    TTransactionId ExternalizeOrReplicateTransaction(
        TTransaction* transaction,
        TCellTagList dstCellTags,
        bool shouldExternalize)
    {
        if (transaction->IsUpload()) {
            return transaction->GetId();
        }

        auto checkTransactionState = [&] (TTransaction* transactionToCheck) {
            auto state = transactionToCheck->GetPersistentState();
            if (state != ETransactionState::Committed && state != ETransactionState::Aborted) {
                return;
            }

            if (transactionToCheck == transaction) {
                YT_LOG_ALERT("Unexpected transaction state encountered while replicating (TransactionId: %v, TransactionState: %v)",
                    transaction->GetId(),
                    state);
            } else {
                YT_LOG_ALERT("Unexpected ancestor transaction state encountered while replicating (TransactionId: %v, AncestorTransactionId: %v, AncestorTransactionState: %v)",
                    transaction->GetId(),
                    transactionToCheck->GetId(),
                    state);
            }
        };

        TCompactVector<std::pair<TTransaction*, TCellTagList>, 16> transactionsToDstCells;
        for (auto* currentTransaction = transaction; currentTransaction; currentTransaction = currentTransaction->GetParent()) {
            YT_VERIFY(IsObjectAlive(currentTransaction));
            checkTransactionState(currentTransaction);

            transactionsToDstCells.emplace_back(currentTransaction, TCellTagList());

            for (auto dstCellTag : dstCellTags) {
                if (shouldExternalize) {
                    if (currentTransaction->IsExternalizedToCell(dstCellTag)) {
                        continue;
                    }
                    currentTransaction->ExternalizedToCellTags().push_back(dstCellTag);
                } else {
                    if (currentTransaction->IsReplicatedToCell(dstCellTag)) {
                        continue;
                    }
                    currentTransaction->ReplicatedToCellTags().push_back(dstCellTag);
                }

                transactionsToDstCells.back().second.push_back(dstCellTag);
            }

            if (transactionsToDstCells.back().second.empty()) {
                // Already present on all dst cells.
                transactionsToDstCells.pop_back();
                break;
            }
        }

        std::reverse(transactionsToDstCells.begin(), transactionsToDstCells.end());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (const auto& [currentTransaction, cellTags] : transactionsToDstCells) {
            auto transactionId = currentTransaction->GetId();
            auto parentTransactionId = GetObjectId(currentTransaction->GetParent());

            auto effectiveTransactionId = transactionId;
            auto effectiveParentTransactionId = parentTransactionId;

            if (shouldExternalize) {
                effectiveTransactionId = MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag());
                effectiveParentTransactionId = MakeExternalizedTransactionId(parentTransactionId, multicellManager->GetCellTag());

                YT_LOG_DEBUG("Externalizing transaction (TransactionId: %v, ParentTransactionId: %v, DstCellTags: %v, ExternalizedTransactionId: %v, ExternalizedParentTransactionId: %v)",
                    transactionId,
                    parentTransactionId,
                    cellTags,
                    effectiveTransactionId,
                    effectiveParentTransactionId);
            } else {
                YT_LOG_DEBUG("Replicating transaction (TransactionId: %v, ParentTransactionId: %v, DstCellTags: %v)",
                    transactionId,
                    parentTransactionId,
                    cellTags);
            }

            // NB: Technically, an externalized transaction *is* foreign, with its native cell being this one.
            // And it *is* coordinated by this cell, even though there's no corresponding 'native' object.

            PostForeignTransactionStart(
                currentTransaction,
                effectiveTransactionId,
                effectiveParentTransactionId,
                cellTags,
                transaction);
        }

        return shouldExternalize
            ? MakeExternalizedTransactionId(transaction->GetId(), multicellManager->GetCellTag())
            : transaction->GetId();
    }

    TTransactionId GetNearestExternalizedTransactionAncestor(
        TTransaction* transaction,
        TCellTag dstCellTag) override
    {
        if (!transaction) {
            return {};
        }

        if (transaction->IsUpload()) {
            return transaction->GetId();
        }

        // NB: The value of this property is the same among all relative
        // transactions.
        auto externalizationEnabled = IsCypressTransactionType(transaction->GetType())
            ? !transaction->IsNative() || transaction->IsNativeTxExternalizationEnabled()
            : false;

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (auto* currentTransaction = transaction; currentTransaction; currentTransaction = currentTransaction->GetParent()) {
            if (externalizationEnabled && currentTransaction->IsExternalizedToCell(dstCellTag)) {
                return MakeExternalizedTransactionId(currentTransaction->GetId(), multicellManager->GetCellTag());
            }

            if (!externalizationEnabled && currentTransaction->IsReplicatedToCell(dstCellTag)) {
                return currentTransaction->GetId();
            }
        }

        return {};
    }

    void PostForeignTransactionStart(
        TTransaction* transaction,
        TTransactionId transactionId,
        TTransactionId parentTransactionId,
        TCellTagList dstCellTags,
        TTransaction* transactionAttributeHolderOverride) override
    {
        NProto::TReqStartForeignTransaction startRequest;

        ToProto(startRequest.mutable_id(), transactionId);

        if (parentTransactionId) {
            ToProto(startRequest.mutable_parent_id(), parentTransactionId);
        }

        if (auto title = transaction->GetTitle()) {
            startRequest.set_title(*title);
        }

        if (IsCypressTransactionType(transaction->GetType())) {
            startRequest.set_enable_native_tx_externalization(transaction->IsNativeTxExternalizationEnabled());
        }

        startRequest.set_upload(transaction->IsUpload());

        if (GetDynamicConfig()->EnableStartForeignTransactionFixes) {
            if (const auto* attributes = transaction->GetAttributes()) {
                if (auto operationType = attributes->Find("operation_type")) {
                    startRequest.set_operation_type(ToProto(ConvertTo<std::string>(operationType)));
                }
                if (auto operationId = attributes->Find("operation_id")) {
                    startRequest.set_operation_id(ToProto(ConvertTo<std::string>(operationId)));
                }
                if (auto operationTitle = attributes->Find("operation_title")) {
                    startRequest.set_operation_title(ToProto(ConvertTo<std::string>(operationTitle)));
                }
            }

            auto* user = transaction->Acd().GetOwner()->AsUser();
            if (IsObjectAlive(user)) {
                startRequest.set_user(ToProto(user->GetName()));
            } else {
                YT_LOG_ALERT("Transaction user is not alive during %v (TransactionId: %v)",
                    transaction->GetId() == transactionId
                    ? "replication"
                    : "externalization",
                    transaction->GetId());
            }
        } else {
            // TODO(shakurov): this is a reproduction of an old bug. Remove.
            const auto* attributes = transactionAttributeHolderOverride
                ? transactionAttributeHolderOverride->GetAttributes()
                : transaction->GetAttributes();
            if (attributes) {
                if (auto operationType = attributes->Find("operation_type")) {
                    startRequest.set_operation_type(ToProto(ConvertTo<std::string>(operationType)));
                }
                if (auto operationId = attributes->Find("operation_id")) {
                    startRequest.set_operation_id(ToProto(ConvertTo<std::string>(operationId)));
                }
                if (auto operationTitle = attributes->Find("operation_title")) {
                    startRequest.set_operation_title(ToProto(ConvertTo<std::string>(operationTitle)));
                }
            }
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMasters(startRequest, dstCellTags);
    }

    TTransaction* GetTransactionOrThrow(TTransactionId transactionId) override
    {
        VerifyPersistentStateRead();

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            ThrowNoSuchTransaction(transactionId);
        }
        return transaction;
    }

    TFuture<TInstant> GetLastPingTime(const TTransaction* transaction) override
    {
        VerifyPersistentStateRead();

        return LeaseTracker_->GetLastPingTime(transaction->GetId());
    }

    void SetTransactionTimeout(
        TTransaction* transaction,
        TDuration timeout) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        transaction->SetTimeout(timeout);

        if (IsLeader()) {
            LeaseTracker_->SetTimeout(transaction->GetId(), timeout);
        }
    }

    void StageObject(TTransaction* transaction, TObject* object) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(transaction->StagedObjects().insert(object).second);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
    }

    void UnstageObject(
        TTransaction* transaction,
        TObject* object,
        bool recursive) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        handler->UnstageObject(object, recursive);

        if (transaction) {
            YT_VERIFY(transaction->StagedObjects().erase(object) == 1);
            objectManager->UnrefObject(object);
        }
    }

    void StageNode(TTransaction* transaction, TCypressNode* trunkNode) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        transaction->StagedNodes().push_back(trunkNode);
        objectManager->RefObject(trunkNode);
    }

    void ImportObject(TTransaction* transaction, TObject* object) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        transaction->ImportedObjects().push_back(object);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
        object->ImportRefObject();
    }

    void DoRegisterTransactionActionHandlers(
        TTransactionActionDescriptor<TTransaction> descriptor) override
    {
        TTransactionManagerBase<TTransaction>::DoRegisterTransactionActionHandlers(
            std::move(descriptor));
    }

    void ExportObject(
        TTransaction* transaction,
        TObject* object,
        TCellTag destinationCellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        transaction->ExportedObjects().push_back({object, destinationCellTag});

        const auto& objectManager = Bootstrap_->GetObjectManager();
        // The remote master cell holds one ref and after object is removed on a remote, it sends TReqUnrefExportedObjects to remove object here.
        objectManager->RefObject(object);
        // The transaction holds the second ref and removes it after completion.
        // This ref is needed to avoid a race between the destruction of an object on a remote cell and the ending of a transaction,
        // since these events come via Hive from different cells.
        objectManager->RefObject(object);

        const auto& handler = objectManager->GetHandler(object);
        handler->ExportObject(object, destinationCellTag);
    }

    std::unique_ptr<TMutation> CreateStartTransactionMutation(
        TCtxStartTransactionPtr context,
        const NTransactionServer::NProto::TReqStartTransaction& request) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            request,
            &TTransactionManager::HydraStartTransaction,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TTransactionManager::HydraRegisterTransactionActions,
            this);
    }

    std::unique_ptr<TMutation> CreateReplicateTransactionsMutation(
        TCtxReplicateTransactionsPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TTransactionManager::HydraReplicateTransactions,
            this);
    }

    std::unique_ptr<TMutation> CreateIssueLeasesMutation(
        TCtxIssueLeasesPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TTransactionManager::HydraIssueLeases,
            this);
    }

    // ITransactionManager implementation.

    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        const std::vector<TCellId>& cellIdsToSyncWith) override
    {
        return GetReadyToPrepareTransactionCommit(
            prerequisiteTransactionIds,
            cellIdsToSyncWith,
            /*transactionIdToRevokeLeases*/ NullTransactionId);
    }

    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        const std::vector<TCellId>& cellIdsToSyncWith,
        TTransactionId transactionIdToRevokeLeases)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (prerequisiteTransactionIds.empty() &&
            cellIdsToSyncWith.empty() &&
            !transactionIdToRevokeLeases)
        {
            return VoidFuture;
        }

        std::vector<TFuture<void>> asyncResults;
        asyncResults.reserve(cellIdsToSyncWith.size() + 2);

        if (!prerequisiteTransactionIds.empty()) {
            asyncResults.push_back(RunTransactionReplicationSession(
                /*syncWithUpstream*/ false,
                Bootstrap_,
                prerequisiteTransactionIds,
                IsMirroringToSequoiaEnabled()));
        }

        if (!cellIdsToSyncWith.empty()) {
            const auto& hiveManager = Bootstrap_->GetHiveManager();
            for (auto cellId : cellIdsToSyncWith) {
                asyncResults.push_back(hiveManager->SyncWith(cellId, true));
            }
        }

        if (transactionIdToRevokeLeases) {
            asyncResults.push_back(RevokeTransactionLeases(transactionIdToRevokeLeases));
        }

        return AllSucceeded(std::move(asyncResults));
    }

    void PrepareTransactionCommit(
        TTransactionId transactionId,
        const TTransactionPrepareOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        PrepareTransactionCommit(transaction, options);
    }

    void PrepareTransactionCommit(
        TTransaction* transaction,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto persistent = options.Persistent;

        // Allow preparing transactions in Active and TransientCommitPrepared (for persistent mode) states.
        // This check applies not only to #transaction itself but also to all of its ancestors.
        {
            auto* currentTransaction = transaction;
            while (currentTransaction) {
                auto state = currentTransaction->GetState(persistent);
                if (state != ETransactionState::Active) {
                    currentTransaction->ThrowInvalidState();
                }
                currentTransaction = currentTransaction->GetParent();
            }
        }

        if (transaction->GetSuccessorTransactionLeaseCount() > 0) {
            ThrowTransactionSuccessorHasLeases(transaction);
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        auto state = transaction->GetState(persistent);
        if (state != ETransactionState::Active) {
            return;
        }

        for (auto prerequisiteTransactionId : options.PrerequisiteTransactionIds) {
            GetAndValidatePrerequisiteTransaction(prerequisiteTransactionId);
        }

        auto sequoiaContextGuard = MaybeCreateSequoiaContextGuard(transaction);

        if (IsSequoiaTxBarrierEnabled() && transaction->IsSequoiaTransaction()) {
            Bootstrap_->GetTransactionSupervisor()->RegisterPreparedSequoiaTx(transaction->GetId());
        }
        RunPrepareTransactionActions(transaction, options);

        if (persistent) {
            transaction->SetPersistentState(ETransactionState::PersistentCommitPrepared);
        } else {
            transaction->SetTransientState(ETransactionState::TransientCommitPrepared);
        }

        YT_LOG_DEBUG(
            "Transaction commit prepared (TransactionId: %v, Persistent: %v, PrepareTimestamp: %v@%v)",
            transaction->GetId(),
            persistent,
            options.PrepareTimestamp,
            options.PrepareTimestampClusterTag);
    }

    void PrepareTransactionAbort(
        TTransactionId transactionId,
        const TTransactionAbortOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetTransientState();
        if (state != ETransactionState::Active && !options.Force) {
            transaction->ThrowInvalidState();
        }
        if (state != ETransactionState::Active) {
            return;
        }

        if (transaction->GetSuccessorTransactionLeaseCount() > 0 && !options.Force) {
            ThrowTransactionSuccessorHasLeases(transaction);
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);
        securityManager->ValidatePermission(transaction, EPermission::Write);

        transaction->SetTransientState(ETransactionState::TransientAbortPrepared);

        YT_LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
            transaction->GetId());
    }

    void CommitTransaction(
        TTransactionId transactionId,
        const TTransactionCommitOptions& options,
        TRevision nativeCommitMutationRevision)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        transaction->SetNativeCommitMutationRevision(nativeCommitMutationRevision);
        CommitTransaction(transaction, options);
    }

    void CommitTransaction(
        TTransactionId transactionId,
        const TTransactionCommitOptions& options) override
    {
        CommitTransaction(
            transactionId,
            options,
            /*nativeCommitMutationRevision*/ NullRevision);
    }

    void AbortTransaction(
        TTransactionId transactionId,
        const TTransactionAbortOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        AbortTransaction(
            transaction,
            options,
            /*validatePermissions*/ true,
            /*replicateViaHive*/ true);
    }

    TFuture<void> PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

    TTransactionId FindUsedNonMirroredTransaction(const TCtxStartCypressTransactionPtr& context)
    {
        TTransactionId mirroredSample, nonMirroredSample;

        auto onCypressTransaction = [&] (TTransactionId transactionId) {
            YT_ASSERT(IsCypressTransactionType(TypeFromId(transactionId)));

            if (IsMirroredToSequoia(transactionId)) {
                mirroredSample = transactionId;
            } else {
                nonMirroredSample = transactionId;
            }
        };

        for (const auto& protoId : context->Request().prerequisite_transaction_ids()) {
            auto transactionId = FromProto<TTransactionId>(protoId);
            if (IsSystemTransactionType(TypeFromId(transactionId))) {
                YT_LOG_ALERT("Attempt to use system transaction as a prerequisite for Cypress one "
                    "(SystemTransactionId: %v)",
                    transactionId);
                THROW_ERROR_EXCEPTION("System transactions cannot be prerequisites for Cypress ones")
                    << TErrorAttribute("system_transaction_id", transactionId);
            } else {
                onCypressTransaction(transactionId);
            }
        }

        auto parentId = FromProto<TTransactionId>(context->Request().parent_id());
        if (parentId) {
            if (IsSystemTransactionType(TypeFromId(parentId))) {
                YT_LOG_ALERT("Attempt to use system transaction as parent for Cypress one "
                    "(SystemTransactionId: %v)",
                    parentId);
                THROW_ERROR_EXCEPTION("System transactions cannot be parent for Cypress ones")
                    << TErrorAttribute("system_transaction_id", parentId);
            } else {
                onCypressTransaction(parentId);
            }
        }

        if (mirroredSample && nonMirroredSample) {
            YT_LOG_ALERT(
                "Attempt to start Cypress transaction which depends on both mirrored and "
                "non-mirrored transaction (MirroredTransactionId: %v, "
                "NonMirroredTransactionId: %v, Title: %v)",
                mirroredSample,
                nonMirroredSample,
                YT_PROTO_OPTIONAL(context->Request(), title));
            THROW_ERROR_EXCEPTION(
                "Cypress transaction cannot depend on both mirrored and non-mirrored "
                "transactions at the same time")
                << TErrorAttribute("mirrored_transaction_id", mirroredSample)
                << TErrorAttribute("non_mirrored_transaction_id", nonMirroredSample);
        }

        return nonMirroredSample;
    }

    void StartCypressTransaction(const TCtxStartCypressTransactionPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (IsMirroringToSequoiaEnabled()) {
            auto nonMirroredTransactionId = FindUsedNonMirroredTransaction(context);
            if (!nonMirroredTransactionId) {
                StartCypressTransactionInSequoiaAndReply(Bootstrap_, context);
                return;
            }

            YT_LOG_WARNING(
                "Mirroring to Sequoia is enabled but non-mirrored Cypress transaction is "
                "started (TransactionId: %v)",
                nonMirroredTransactionId);
        }

        auto hydraRequest = BuildStartCypressTransactionRequest(
            std::move(context->Request()),
            context->GetAuthenticationIdentity());

        auto mutation = CreateStartCypressTransactionMutation(context, hydraRequest);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void CommitCypressTransaction(const TCtxCommitCypressTransactionPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& request = context->Request();

        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto* transaction = GetTransactionOrThrow(transactionId);

        YT_VERIFY(transaction->GetIsCypressTransaction());

        // TODO(kvk1920): optimize.
        // For mirrored transactions it's enough to lock some rows in
        // "transactions" Sequoia table. Replication isn't necessary here.
        std::vector<TTransactionId> prerequisiteTransactionIds;
        if (context->GetRequestHeader().HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
            auto* prerequisitesExt = &context->GetRequestHeader().GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            const auto& prerequisiteTransactions = prerequisitesExt->transactions();
            prerequisiteTransactionIds.reserve(prerequisiteTransactions.size());
            for (const auto& prerequisite : prerequisiteTransactions) {
                prerequisiteTransactionIds.push_back(FromProto<TTransactionId>(prerequisite.transaction_id()));
            }
        }

        auto revokeLeases = transaction->GetSuccessorTransactionLeaseCount() > 0;

        auto readyEvent = GetReadyToPrepareTransactionCommit(
            prerequisiteTransactionIds,
            /*cellIdsToSyncWith*/ {},
            revokeLeases ? transactionId : NullTransactionId);

        TFuture<TSharedRefArray> responseFuture;
        // Fast path.
        if (readyEvent.IsSet() && readyEvent.Get().IsOK()) {
            responseFuture = DoCommitTransaction(
                transactionId,
                prerequisiteTransactionIds,
                context->GetMutationId(),
                context->IsRetry(),
                /*prepareError*/ {});
        } else {
            responseFuture = readyEvent.Apply(
                BIND(
                    &TTransactionManager::DoCommitTransaction,
                    MakeStrong(this),
                    transactionId,
                    prerequisiteTransactionIds,
                    context->GetMutationId(),
                    context->IsRetry())
                    .AsyncVia(EpochAutomatonInvoker_));
        }

        context->ReplyFrom(responseFuture);
    }

    // COMPAT(h0pless): Remove this after CTxS will be used by clients to manipulate Cypress transactions.
    bool CommitTransaction(TCtxCommitTransactionPtr context) override
    {
        if (GetDynamicConfig()->IgnoreCypressTransactions) {
            return false;
        }

        const auto& request = context->Request();
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto* transaction = FindTransaction(transactionId);
        if (!transaction || !transaction->GetIsCypressTransaction()) {
            return false;
        }

        const auto& mutationId = context->GetMutationId();
        if (mutationId) {
            const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
            if (auto result = responseKeeper->FindRequest(mutationId, context->IsRetry())) {
                context->ReplyFrom(std::move(result));
                return true;
            }
        }

        auto participantCellIds = FromProto<std::vector<TCellId>>(request.participant_cell_ids());
        auto force2PC = request.force_2pc();
        if (request.force_2pc() || !participantCellIds.empty()) {
            THROW_ERROR_EXCEPTION("Cypress transactions cannot be committed via 2PC")
                << TErrorAttribute("transaction_id", transactionId)
                << TErrorAttribute("force_2pc", force2PC)
                << TErrorAttribute("participant_cell_ids", participantCellIds);
        }

        std::vector<TTransactionId> prerequisiteTransactionIds;
        if (context->GetRequestHeader().HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
            auto* prerequisitesExt = &context->GetRequestHeader().GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            const auto& prerequisiteTransactions = prerequisitesExt->transactions();
            prerequisiteTransactionIds.reserve(prerequisiteTransactions.size());
            for (const auto& prerequisite : prerequisiteTransactions) {
                prerequisiteTransactionIds.push_back(FromProto<TTransactionId>(prerequisite.transaction_id()));
            }
        }

        auto revokeLeases = transaction->GetSuccessorTransactionLeaseCount() > 0;

        auto readyEvent = GetReadyToPrepareTransactionCommit(
            prerequisiteTransactionIds,
            /*cellIdsToSyncWith*/ {},
            revokeLeases ? transactionId : NullTransactionId);

        TFuture<TSharedRefArray> responseFuture;
        // Fast path.
        if (readyEvent.IsSet() && readyEvent.Get().IsOK()) {
            responseFuture = DoCommitTransaction(
                transactionId,
                prerequisiteTransactionIds,
                mutationId,
                context->IsRetry(),
                /*prepareError*/ {});
        } else {
            responseFuture = readyEvent.Apply(
                BIND(
                    &TTransactionManager::DoCommitTransaction,
                    MakeStrong(this),
                    transactionId,
                    prerequisiteTransactionIds,
                    mutationId,
                    context->IsRetry())
                    .AsyncVia(EpochAutomatonInvoker_));
        }

        context->ReplyFrom(responseFuture);
        return true;
    }

    TFuture<TSharedRefArray> DoCommitTransaction(
        TTransactionId transactionId,
        std::vector<TTransactionId> prerequisiteTransactionIds,
        NRpc::TMutationId mutationId,
        bool isRetry,
        const TError& prepareError)
    {
        if (!prepareError.IsOK()) {
            auto error = TError("Failed to get ready for transaction commit")
                << prepareError;
            return MakeFuture<TSharedRefArray>(error);
        }

        const auto& timestampProvider = Bootstrap_->GetTimestampProvider();
        auto asyncTimestamp = timestampProvider->GenerateTimestamps();
        return asyncTimestamp.Apply(
            BIND(
                &TTransactionManager::OnCommitTimestampGenerated,
                MakeStrong(this),
                transactionId,
                prerequisiteTransactionIds,
                std::move(mutationId),
                isRetry)
                    .AsyncVia(EpochAutomatonInvoker_));
    }

    TFuture<TSharedRefArray> OnCommitTimestampGenerated(
        TTransactionId transactionId,
        std::vector<TTransactionId> prerequisiteTransactionIds,
        NRpc::TMutationId mutationId,
        bool isRetry,
        const TErrorOr<TTimestamp>& timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            auto error = TError("Failed to generate commit timestamp")
                << timestampOrError;
            return MakeFuture<TSharedRefArray>(TError(timestampOrError));
        }

        auto commitTimestamp = timestampOrError.Value();
        YT_LOG_DEBUG("Commit timestamp generated for transaction "
            "(TransactionId: %v, CommitTimestamp: %v)",
            transactionId,
            commitTimestamp);

        if (IsMirroringToSequoiaEnabled() && IsMirroredToSequoia(transactionId)) {
            return CommitCypressTransactionInSequoia(
                Bootstrap_,
                transactionId,
                std::move(prerequisiteTransactionIds),
                commitTimestamp,
                NRpc::GetCurrentAuthenticationIdentity());
        }

        auto request = BuildCommitCypressTransactionRequest(
            transactionId,
            commitTimestamp,
            prerequisiteTransactionIds,
            NRpc::GetCurrentAuthenticationIdentity());

        auto mutation = CreateMutation(HydraManager_, request);
        mutation->SetMutationId(mutationId, isRetry);
        mutation->SetCurrentTraceContext();
        return mutation->Commit().Apply(BIND([=] (const TMutationResponse& rsp) {
            return rsp.Data;
        }));
    }

    void AbortCypressTransaction(const TCtxAbortCypressTransactionPtr& context) override
    {
        const auto& rpcRequest = context->Request();
        auto transactionId = FromProto<TTransactionId>(rpcRequest.transaction_id());

        if (IsMirroringToSequoiaEnabled() && IsMirroredToSequoia(transactionId)) {
            AbortCypressTransactionInSequoiaAndReply(Bootstrap_, context);
            return;
        }

        auto force = rpcRequest.force();
        auto* transaction = GetTransactionOrThrow(transactionId);

        YT_VERIFY(transaction->GetIsCypressTransaction());

        auto request = BuildAbortCypressTransactionRequest(
            transactionId,
            force,
            /*replicateViaHive*/ true,
            NRpc::GetCurrentAuthenticationIdentity());

        auto mutation = CreateMutation(HydraManager_, request);
        context->ReplyFrom(DoAbortTransaction(
            std::move(mutation),
            transaction,
            force));
    }

    // COMPAT(h0pless): Remove this after CTxS will be used by clients to manipulate Cypress transactions.
    bool AbortTransaction(TCtxAbortTransactionPtr context) override
    {
        if (GetDynamicConfig()->IgnoreCypressTransactions) {
            return false;
        }

        const auto& request = context->Request();
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto force = request.force();
        auto* transaction = FindTransaction(transactionId);
        if (!transaction || !transaction->GetIsCypressTransaction()) {
            return false;
        }

        auto mutationId = context->GetMutationId();
        if (mutationId) {
            const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
            if (auto result = responseKeeper->FindRequest(mutationId, context->IsRetry())) {
                context->ReplyFrom(std::move(result));
                return true;
            }
        }

        NProto::TReqAbortCypressTransaction req;
        ToProto(req.mutable_transaction_id(), transactionId);
        req.set_force(force);
        WriteAuthenticationIdentityToProto(&req, NRpc::GetCurrentAuthenticationIdentity());

        auto mutation = CreateMutation(HydraManager_, req);
        mutation->SetMutationId(mutationId, context->IsRetry());
        mutation->SetCurrentTraceContext();

        context->ReplyFrom(DoAbortTransaction(
            std::move(mutation),
            transaction,
            force));
        return true;
    }

    TFuture<TSharedRefArray> DoAbortTransaction(
        std::unique_ptr<TMutation> abortMutation,
        TTransaction* transaction,
        bool force)
    {
        auto transactionId = transaction->GetId();

        // Fast path.
        if (force || transaction->GetSuccessorTransactionLeaseCount() == 0) {
            return OnTransactionAbortPrepared(
                std::move(abortMutation),
                /*error*/ {});
        } else {
            return RevokeTransactionLeases(transactionId).Apply(
                BIND(
                    &TTransactionManager::OnTransactionAbortPrepared,
                    MakeStrong(this),
                    Passed(std::move(abortMutation)))
                    .AsyncVia(EpochAutomatonInvoker_));
        }
    }

    TFuture<TSharedRefArray> OnTransactionAbortPrepared(
        std::unique_ptr<TMutation> abortMutation,
        const TError& error)
    {
        if (!error.IsOK()) {
            return MakeFuture<TSharedRefArray>(error);
        }

        return abortMutation->Commit().Apply(BIND([=] (const TMutationResponse& rsp) {
            return rsp.Data;
        }));
    }

    TTransaction* GetAndValidatePrerequisiteTransaction(TTransactionId transactionId) override
    {
        auto* prerequisiteTransaction = FindTransaction(transactionId);
        if (!IsObjectAlive(prerequisiteTransaction)) {
            THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is missing",
                transactionId);
        }
        if (prerequisiteTransaction->GetPersistentState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is in %Qlv state",
                transactionId,
                prerequisiteTransaction->GetPersistentState());
        }

        return prerequisiteTransaction;
    }

    void CreateOrRefTimestampHolder(TTransactionId transactionId) override
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            ++it->second.RefCount;
        }
        TimestampHolderMap_.emplace(transactionId, TTimestampHolder{});
    }

    void SetTimestampHolderTimestamp(TTransactionId transactionId, TTimestamp timestamp) override
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            it->second.Timestamp = timestamp;
        }
    }

    TTimestamp GetTimestampHolderTimestamp(TTransactionId transactionId) override
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            return it->second.Timestamp;
        }
        return NullTimestamp;
    }

    void UnrefTimestampHolder(TTransactionId transactionId) override
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            --it->second.RefCount;
            if (it->second.RefCount == 0) {
                TimestampHolderMap_.erase(it);
            }
        }
    }

    TEntityMap<TTransaction>* MutableTransactionMap() override
    {
        return &TransactionMap_;
    }

    const THashSet<TTransaction*>& ForeignTransactions() const override
    {
        return ForeignTransactions_;
    }

    const THashSet<TTransaction*>& NativeTopmostTransactions() const override
    {
        return NativeTopmostTransactions_;
    }

    const THashSet<TTransaction*>& NativeTransactions() const override
    {
        return NativeTransactions_;
    }

private:
    struct TTimestampHolder
    {
        TTimestamp Timestamp = NullTimestamp;
        i64 RefCount = 1;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using ::NYT::Persist;
            Persist(context, Timestamp);
            Persist(context, RefCount);
        }
    };

    using TCtxCommitCypressTransaction = NRpc::TTypedServiceContext<
        NProto::TReqCommitCypressTransaction,
        TRspCommitTransaction>;
    using TCtxCommitCypressTransactionPtr = TIntrusivePtr<TCtxCommitCypressTransaction>;

    using TCtxAbortCypressTransaction = NRpc::TTypedServiceContext<
        NProto::TReqAbortCypressTransaction,
        TRspCommitTransaction>;
    using TCtxAbortCypressTransactionPtr = TIntrusivePtr<TCtxAbortCypressTransaction>;

    friend class TTransactionTypeHandler;

    const TBoomerangTrackerPtr BoomerangTracker_;

    NProfiling::TBufferedProducerPtr BufferedProducer_;
    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    const ITransactionLeaseTrackerPtr LeaseTracker_;

    TEntityMap<TTransaction> TransactionMap_;

    THashMap<TTransactionId, TTimestampHolder> TimestampHolderMap_;

    THashSet<TTransaction*> ForeignTransactions_;
    THashSet<TTransaction*> NativeTopmostTransactions_;
    THashSet<TTransaction*> NativeTransactions_;

    // COMPAT(aleksandra-zh).
    bool FixExportedObjectsRefs_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    // This should become a mutation used to create system transactions only.
    void HydraStartTransaction(
        const TCtxStartTransactionPtr& context,
        NTransactionServer::NProto::TReqStartTransaction* request,
        NTransactionServer::NProto::TRspStartTransaction* response)
    {
        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, std::move(identity));

        // COMPAT(h0pless): This should always be false when clients will switch to Cypress tx service from tx service.
        auto isCypressTransaction = request->is_cypress_transaction();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        if (!isCypressTransaction) {
            auto* schema = objectManager->GetSchema(EObjectType::SystemTransaction);
            securityManager->ValidatePermission(schema, EPermission::Create);
        } else {
            auto* schema = objectManager->GetSchema(EObjectType::Transaction);
            securityManager->ValidatePermission(schema, EPermission::Create);
        }

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto* parent = parentId ? GetTransactionOrThrow(parentId) : nullptr;

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        std::vector<TTransaction*> prerequisiteTransactions;
        for (auto id : prerequisiteTransactionIds) {
            auto* prerequisiteTransaction = GetAndValidatePrerequisiteTransaction(id);
            prerequisiteTransactions.push_back(prerequisiteTransaction);
        }

        auto attributes = request->has_attributes()
            ? FromProto(request->attributes())
            : CreateEphemeralAttributes();

        auto title = request->has_title() ? std::optional(request->title()) : std::nullopt;

        auto timeout = FromProto<TDuration>(request->timeout());

        std::optional<TInstant> deadline;
        if (request->has_deadline()) {
            deadline = FromProto<TInstant>(request->deadline());
        }

        auto replicateToCellTags = FromProto<TCellTagList>(request->replicate_to_cell_tags());
        auto* transaction = StartTransaction(
            parent,
            prerequisiteTransactions,
            replicateToCellTags,
            timeout,
            deadline,
            title,
            *attributes,
            isCypressTransaction);

        auto id = transaction->GetId();

        if (response) {
            ToProto(response->mutable_id(), id);
        }

        if (context) {
            context->SetResponseInfo("TransactionId: %v", id);
        }
    }

    void HydraPrepareAndCommitStartCypressTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NTransactionServer::NProto::TReqStartCypressTransaction* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        HydraStartCypressTransaction(/*context*/ nullptr, request, /*response*/ nullptr);
    }

    void HydraStartCypressTransaction(
        const TCtxStartCypressTransactionPtr& context,
        NTransactionServer::NProto::TReqStartCypressTransaction* request,
        NProto::TRspStartCypressTransaction* response)
    {
        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, std::move(identity));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(EObjectType::Transaction);
        securityManager->ValidatePermission(schema, EPermission::Create);

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto* parent = parentId ? GetTransactionOrThrow(parentId) : nullptr;

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        std::vector<TTransaction*> prerequisiteTransactions;
        for (auto id : prerequisiteTransactionIds) {
            auto* prerequisiteTransaction = GetAndValidatePrerequisiteTransaction(id);
            prerequisiteTransactions.push_back(prerequisiteTransaction);
        }

        auto attributes = request->has_attributes()
            ? FromProto(request->attributes())
            : CreateEphemeralAttributes();

        auto title = YT_PROTO_OPTIONAL(*request, title);
        auto timeout = FromProto<TDuration>(request->timeout());
        auto deadline = YT_PROTO_OPTIONAL(*request, deadline, TInstant);
        auto hintId = YT_PROTO_OPTIONAL(*request, hint_id, TTransactionId)
            .value_or(TTransactionId{});

        auto replicateToCellTags = FromProto<TCellTagList>(request->replicate_to_cell_tags());
        auto* transaction = StartTransaction(
            parent,
            prerequisiteTransactions,
            replicateToCellTags,
            timeout,
            deadline,
            title,
            *attributes,
            /*isCypressTransaction*/ true,
            hintId);

        auto id = transaction->GetId();

        if (response) {
            ToProto(response->mutable_id(), id);
        }

        if (context) {
            context->SetResponseInfo("TransactionId: %v", id);
        }
    }

    void HydraCommitStartForeignTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqStartForeignTransaction* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto hintId = FromProto<TTransactionId>(request->id());
        YT_VERIFY(IsSequoiaId(hintId));

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        YT_VERIFY(!parentId || IsSequoiaId(parentId));

        HydraStartForeignTransaction(request);
    }

    void HydraStartForeignTransaction(NProto::TReqStartForeignTransaction* request)
    {
        auto hintId = FromProto<TTransactionId>(request->id());

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto* parent = parentId ? FindTransaction(parentId) : nullptr;
        auto isUpload = request->upload();
        if (parentId && !parent) {
            THROW_ERROR_EXCEPTION("Failed to start foreign transaction: parent transaction not found")
                << TErrorAttribute("transaction_id", hintId)
                << TErrorAttribute("parent_transaction_id", parentId);
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetRootUser();
        // COMPAT(shakurov): in 24.2, user will never be null.
        if (request->has_user()) {
            const auto& userName = request->user();
            user = securityManager->FindUserByName(userName, true /*activeLifeStageOnly*/);
            if (!IsObjectAlive(user)) {
                YT_LOG_ALERT("Foreign transaction user not found, falling back to root (User: %v)",
                    userName);
                user = securityManager->GetRootUser();
            }
        }

        // NB: DoStartTransaction below cares about authenticated user.
        TAuthenticatedUserGuard userGuard(securityManager, user);

        auto title = YT_PROTO_OPTIONAL(*request, title);

        YT_VERIFY(
            isUpload == (
                TypeFromId(hintId) == EObjectType::UploadTransaction ||
                TypeFromId(hintId) == EObjectType::UploadNestedTransaction));

        auto attributes = CreateEphemeralAttributes();
        if (request->has_operation_type()) {
            attributes->Set("operation_type", request->operation_type());
        }
        if (request->has_operation_id()) {
            attributes->Set("operation_id", request->operation_id());
        }
        if (request->has_operation_title()) {
            attributes->Set("operation_title", request->operation_title());
        }

        auto transactionType = TypeFromId(hintId);
        auto* transaction = DoStartTransaction(
            isUpload,
            parent,
            /*prerequisiteTransactions*/ {},
            /*replicatedToCellTags*/ {},
            /*timeout*/ std::nullopt,
            /*deadline*/ std::nullopt,
            title,
            *attributes,
            IsCypressTransactionType(transactionType),
            IsCypressTransactionType(transactionType)
                ? std::optional(request->enable_native_tx_externalization())
                : std::nullopt,
            hintId);
        YT_VERIFY(transaction->GetId() == hintId);
    }

    void HydraRegisterTransactionActions(
        const TCtxRegisterTransactionActionsPtr& /*context*/,
        TReqRegisterTransactionActions* request,
        TRspRegisterTransactionActions* /*response*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        if (GetDynamicConfig()->ForbidTransactionActionsForCypressTransactions) {
            if (transaction->GetIsCypressTransaction() && !request->actions().empty()) {
                YT_LOG_ALERT(
                    "Transaction is Cypress but has actions (TransactionId: %v, TransactionActionCount: %v)",
                    transaction->GetId(),
                    request->actions_size());
                THROW_ERROR_EXCEPTION("Cypress transactions cannot have transaction actions");
            }
        }

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);

            YT_LOG_DEBUG("Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                data.Type);
        }
    }

    void HydraPrepareTransactionCommit(NProto::TReqPrepareTransactionCommit* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prepareTimestamp = request->prepare_timestamp();
        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, std::move(identity));

        TTransactionPrepareOptions options{
            .Persistent = true,
            .PrepareTimestamp = prepareTimestamp,
        };
        PrepareTransactionCommit(transactionId, options);
    }

    void HydraCommitCommitForeignTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqCommitTransaction* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_VERIFY(IsSequoiaId(transactionId));

        HydraCommitTransaction(request);
    }

    void HydraCommitTransaction(NProto::TReqCommitTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamp = request->commit_timestamp();
        auto nativeCommitMutationRevision = FromProto<NHydra::TRevision>(request->native_commit_mutation_revision());

        TTransactionCommitOptions options{
            .CommitTimestamp = commitTimestamp,
        };
        CommitTransaction(transactionId, options, nativeCommitMutationRevision);
    }

    void HydraCommitAbortForeignTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqAbortTransaction* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_VERIFY(IsSequoiaId(transactionId));
        YT_VERIFY(request->force());

        HydraAbortTransaction(request);
    }

    void HydraAbortTransaction(NProto::TReqAbortTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAbortOptions options{
            .Force = request->force(),
        };
        AbortTransaction(transactionId, options);
    }

    void HydraPrepareAndCommitCommitCypressTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqCommitCypressTransaction* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_VERIFY(IsMirroredToSequoia(transactionId));

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());

        try {
            PrepareAndCommitCypressTransaction(
                transaction,
                prerequisiteTransactionIds,
                request->commit_timestamp(),
                /*replicateViaHive*/ false);
        } catch (const std::exception& ex) {
            // Abort it reliably.
            // TODO(kvk1920): proper way to abort mirrored tx from master.
            SetTransactionTimeout(transaction, TDuration::Zero());
        }
    }

    void PrepareAndCommitCypressTransaction(
        TTransaction* transaction,
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        TTimestamp commitTimestamp,
        bool replicateViaHive)
    {
        TTransactionPrepareOptions prepareOptions{
            .Persistent = true,
            .LatePrepare = true, // Technically true.
            .PrepareTimestamp = commitTimestamp,
            .PrepareTimestampClusterTag = Bootstrap_->GetPrimaryCellTag(),
            .PrerequisiteTransactionIds = prerequisiteTransactionIds,
        };
        PrepareTransactionCommit(transaction, prepareOptions);

        TTransactionCommitOptions commitOptions{
            .CommitTimestamp = commitTimestamp,
            .CommitTimestampClusterTag = Bootstrap_->GetPrimaryCellTag(),
        };
        CommitTransaction(transaction, commitOptions, replicateViaHive);
    }

    void HydraCommitCypressTransaction(
        const TCtxCommitCypressTransactionPtr& /*context*/,
        NProto::TReqCommitCypressTransaction* request,
        TRspCommitTransaction* /*response*/)
    {
        YT_VERIFY(HasHydraContext());

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto* transaction = GetTransactionOrThrow(transactionId);

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());

        try {
            PrepareAndCommitCypressTransaction(
                transaction,
                prerequisiteTransactionIds,
                request->commit_timestamp(),
                /*replicateViaHive*/ true);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to commit transaction, aborting (TransactionId: %v)",
                transactionId);

            TTransactionAbortOptions abortOptions{
                .Force = true,
            };
            AbortTransaction(
                transaction,
                abortOptions,
                /*validatePermissions*/ true,
                /*replicateViaHive*/ true);

            throw;
        }
    }

    void HydraPrepareAndCommitAbortCypressTransactionInSequoia(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqAbortCypressTransaction* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_VERIFY(IsMirroredToSequoia(transactionId));
        YT_VERIFY(!request->replicate_via_hive());

        HydraAbortCypressTransaction(/*context*/ nullptr, request, /*response*/ nullptr);
    }

    void HydraAbortCypressTransaction(
        const TCtxAbortCypressTransactionPtr& /*context*/,
        NProto::TReqAbortCypressTransaction* request,
        TRspAbortTransaction* /*response*/)
    {
        YT_VERIFY(HasHydraContext());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto* transaction = GetTransactionOrThrow(transactionId);

        auto force = request->force();

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active && !force) {
            transaction->ThrowInvalidState();
        }

        if (state != ETransactionState::Active) {
            return;
        }

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);
        securityManager->ValidatePermission(transaction, EPermission::Write);

        TTransactionAbortOptions abortOptions{
            .Force = force,
        };
        AbortTransaction(
            transaction,
            abortOptions,
            /*validatePermissions*/ true,
            request->replicate_via_hive());
    }

    void HydraCommitMarkCypressTransactionsReplicatedToCell(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqMarkCypressTransactionsReplicatedToCells* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto destinationCellTags = FromProto<TCellTagList>(request->destination_cell_tags());

        for (const auto& protoId : request->transaction_ids()) {
            auto transactionId = FromProto<TTransactionId>(protoId);
            if (auto* transaction = FindTransaction(transactionId)) {
                MarkTransactionReplicated(transaction, destinationCellTags);
            }
        }
    }

    void MaterializeCypressTransactionReplica(NProto::TReqStartForeignTransaction* request)
    {
        YT_VERIFY(HasMutationContext());

        auto transactionId = FromProto<TTransactionId>(request->id());
        if (auto* transaction = FindTransaction(transactionId)) {
            THROW_ERROR_EXCEPTION_IF(transaction->GetPersistentState() != ETransactionState::Active,
                "Transaction already exists but is not active");

            // Concurrent tx replication should not lead to lock conflict.
            return;
        }

        HydraStartForeignTransaction(request);
    }

    void HydraCommitMaterializeCypressTransactionReplicas(
        TTransaction* /*sequoiaTransaction*/,
        NProto::TReqMaterializeCypressTransactionReplicas* request,
        const TTransactionCommitOptions& /*options*/)
    {
        for (auto& subrequest : *request->mutable_transactions()) {
            try {
                MaterializeCypressTransactionReplica(&subrequest);
            } catch (const std::exception& ex) {
                YT_LOG_ALERT(
                    ex,
                    "Failed to prepare Cypress transaction replica materialization "
                    "(TransactionId: %v)",
                    FromProto<TTransactionId>(subrequest.id()));
            }
        }
    }

    void HydraReplicateTransactions(
        const TCtxReplicateTransactionsPtr& context,
        TReqReplicateTransactions* request,
        TRspReplicateTransactions* response)
    {
        auto destinationCellTag = static_cast<TCellTag>(request->destination_cell_tag());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        TCompactVector<TTransactionId, 4> postedTransactionIds;
        TCompactVector<TTransactionId, 4> skippedTransactionIds;
        TCompactVector<TTransactionId, 4> postedMissingTransactionIds;
        for (const auto& protoTransactionId : request->transaction_ids()) {
            auto transactionId = FromProto<TTransactionId>(protoTransactionId);
            YT_VERIFY(CellTagFromId(transactionId) == Bootstrap_->GetCellTag());
            auto* transaction = FindTransaction(transactionId);

            if (!IsObjectAlive(transaction)) {
                NProto::TReqNoteNoSuchTransaction noSuchTransactionRequest;
                ToProto(noSuchTransactionRequest.mutable_id(), transactionId);
                multicellManager->PostToMaster(noSuchTransactionRequest, destinationCellTag);

                postedMissingTransactionIds.push_back(transactionId);

                continue;
            }

            YT_VERIFY(transaction->IsNative());

            if (transaction->IsReplicatedToCell(destinationCellTag)) {
                skippedTransactionIds.push_back(transactionId);
                // Don't post anything.
                continue;
            }

            ReplicateTransaction(transaction, {destinationCellTag});
            YT_VERIFY(transaction->IsReplicatedToCell(destinationCellTag));

            postedTransactionIds.push_back(transactionId);
        }

        response->set_sync_implied(!postedTransactionIds.empty());

        // NB: may be empty.
        auto boomerangWaveId = FromProto<TBoomerangWaveId>(request->boomerang_wave_id());
        YT_ASSERT(!boomerangWaveId ||
            (request->has_boomerang_wave_id() &&
            request->has_boomerang_wave_size() &&
            request->has_boomerang_mutation_id() &&
            request->has_boomerang_mutation_type() &&
            request->has_boomerang_mutation_data()));
        auto boomerangMutationId = request->has_boomerang_mutation_id()
            ? FromProto<NRpc::TMutationId>(request->boomerang_mutation_id())
            : NRpc::TMutationId();
        auto boomerangWaveSize = request->boomerang_wave_size();

        if (boomerangWaveId) {
            NProto::TReqReturnBoomerang boomerangRequest;

            boomerangRequest.mutable_boomerang_wave_id()->Swap(request->mutable_boomerang_wave_id());
            boomerangRequest.set_boomerang_wave_size(request->boomerang_wave_size());

            boomerangRequest.mutable_boomerang_mutation_id()->Swap(request->mutable_boomerang_mutation_id());
            boomerangRequest.set_boomerang_mutation_type(request->boomerang_mutation_type());
            boomerangRequest.set_boomerang_mutation_data(request->boomerang_mutation_data());

            multicellManager->PostToMaster(boomerangRequest, destinationCellTag);
        }

        if (context) {
            context->SetResponseInfo(
                "ReplicatedTransactionIds: %v, MissingTransactionIds: %v, SkippedTransactionIds: %v, "
                "BoomerangMutationId: %v, BoomerangWaveId: %v, BoomerangWaveSize: %v",
                postedTransactionIds,
                postedMissingTransactionIds,
                skippedTransactionIds,
                boomerangMutationId,
                boomerangWaveId,
                boomerangWaveSize);
        }
    }

    void HydraNoteNoSuchTransaction(NProto::TReqNoteNoSuchTransaction* request)
    {
        // NB: This has no effect on the persistent state, but it does notify
        // transient subscribers and does cache transaction absence.
        auto transactionId = FromProto<TTransactionId>(request->id());
        CacheTransactionFinished(transactionId);
    }

    void HydraReturnBoomerang(NProto::TReqReturnBoomerang* request)
    {
        BoomerangTracker_->ProcessReturnedBoomerang(request);
    }

    void HydraRemoveStuckBoomerangWaves(NProto::TReqRemoveStuckBoomerangWaves* request)
    {
        BoomerangTracker_->RemoveStuckBoomerangWaves(request);
    }

    void HydraIssueLeases(
        const TCtxIssueLeasesPtr& /*context*/,
        NProto::TReqIssueLeases* request,
        NProto::TRspIssueLeases* /*response*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto transactionIds = FromProto<std::vector<TTransactionId>>(request->transaction_ids());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto cellId = FromProto<TCellId>(request->cell_id());
        auto* cell = cellManager->GetCellOrThrow(cellId);

        const auto& hiveManager = Bootstrap_->GetHiveManager();

        for (auto transactionId : transactionIds) {
            auto* transaction = GetTransactionOrThrow(transactionId);
            if (transaction->GetPersistentState() != ETransactionState::Active) {
                transaction->ThrowInvalidState();
            }
            if (!transaction->GetIsCypressTransaction()) {
                THROW_ERROR_EXCEPTION("Leases cannot be issued for non-Cypress transactions")
                    << TErrorAttribute("transaction_id", transaction->GetId());
            }
            if (transaction->GetTransactionLeasesState() != ETransactionLeasesState::Active) {
                THROW_ERROR_EXCEPTION("Transaction is revoking leases")
                    << TErrorAttribute("transaction_id", transaction->GetId())
                    << TErrorAttribute("transaction_leases_state", transaction->GetTransactionLeasesState());
            }

            if (RegisterTransactionLease(transaction, cell)) {
                NLeaseServer::NProto::TReqRegisterLease message;
                ToProto(message.mutable_lease_id(), transaction->GetId());

                auto mailbox = hiveManager->GetOrCreateCellMailbox(cellId);
                hiveManager->PostMessage(mailbox, message);
            }
        }
    }

    void HydraRevokeLeases(NProto::TReqRevokeLeases* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto* transaction = FindTransaction(transactionId);
        if (!transaction) {
            YT_LOG_DEBUG(
                "Requested to revoke leases for non-existent transaction, ignored (TransactionId: %v)",
                transactionId);
        }

        RevokeLeases(transaction, /*force*/ false);
    }

    void RevokeLeases(TTransaction* transaction, bool force)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        auto revokeTransaction = [&] (TTransaction* transaction) {
            YT_LOG_DEBUG(
                "Revoking leases for transaction "
                "(TransactionId: %v, TransactionLeaseCount: %v, SuccessorTransactionLeaseCount: %v)",
                transaction->GetId(),
                transaction->LeaseCellIds().size(),
                transaction->GetSuccessorTransactionLeaseCount());

            transaction->SetTransactionLeasesState(ETransactionLeasesState::Revoking);

            auto leaseCellIds = std::vector(
                transaction->LeaseCellIds().begin(),
                transaction->LeaseCellIds().end());
            Sort(leaseCellIds);
            for (auto cellId : leaseCellIds) {
                NLeaseServer::NProto::TReqRevokeLease message;
                ToProto(message.mutable_lease_id(), transaction->GetId());
                message.set_force(force);

                auto mailbox = hiveManager->GetOrCreateCellMailbox(cellId);
                hiveManager->PostMessage(mailbox, message);

                if (force) {
                    auto* cell = cellManager->GetCell(cellId);
                    UnregisterTransactionLease(transaction, cell);
                }
            }
        };
        IterateSuccessorTransactions(transaction, BIND(revokeTransaction));
    }

public:
    void FinishTransaction(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        for (auto* object : transaction->StagedObjects()) {
            const auto& handler = objectManager->GetHandler(object);
            handler->UnstageObject(object, false);
            objectManager->UnrefObject(object);
        }
        transaction->StagedObjects().clear();

        for (auto* node : transaction->StagedNodes()) {
            objectManager->UnrefObject(node);
        }
        transaction->StagedNodes().clear();

        auto* parent = transaction->GetParent();
        if (parent) {
            EraseOrCrash(parent->NestedTransactions(), transaction);
            objectManager->UnrefObject(transaction);
            transaction->SetParent(nullptr);
        }

        YT_VERIFY(transaction->GetSuccessorTransactionLeaseCount() == 0);

        if (transaction->IsNative()) {
            EraseOrCrash(NativeTransactions_, transaction);
            if (!parent) {
                EraseOrCrash(NativeTopmostTransactions_, transaction);
            }
        } else {
            EraseOrCrash(ForeignTransactions_, transaction);
        }

        for (auto* prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            // NB: Duplicates are fine; prerequisite transactions may be duplicated.
            prerequisiteTransaction->DependentTransactions().erase(transaction);
        }
        transaction->PrerequisiteTransactions().clear();

        TCompactVector<TTransaction*, 16> dependentTransactions(
            transaction->DependentTransactions().begin(),
            transaction->DependentTransactions().end());
        std::sort(dependentTransactions.begin(), dependentTransactions.end(), TObjectIdComparer());
        for (auto* dependentTransaction : dependentTransactions) {
            if (!IsObjectAlive(dependentTransaction)) {
                continue;
            }
            if (dependentTransaction->GetPersistentState() != ETransactionState::Active) {
                continue;
            }
            YT_LOG_DEBUG("Aborting dependent transaction (DependentTransactionId: %v, PrerequisiteTransactionId: %v)",
                dependentTransaction->GetId(),
                transaction->GetId());
            TTransactionAbortOptions options{
                .Force = true,
            };
            // Some dependent transactions may be not mirrored to Sequoia
            // Ground so their abort have to be replicated via Hive.
            bool suppressReplicationViaHive =
                IsMirroringToSequoiaEnabled() &&
                IsMirroredToSequoia(dependentTransaction->GetId());
            AbortTransaction(
                dependentTransaction,
                options,
                /*validatePermissions*/ false,
                /*replicateViaHive*/ !suppressReplicationViaHive);
        }
        transaction->DependentTransactions().clear();

        transaction->SetDeadline(std::nullopt);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetTransactionAccountResourceUsage(transaction);

        CacheTransactionFinished(transaction);

        // Kill the fake reference thus destroying the object.
        objectManager->UnrefObject(transaction);
    }

private:
    // Cf. TTransactionPresenceCache::GetTransactionPresence
    bool ShouldCacheTransactionPresence(TTransaction* transaction)
    {
        YT_ASSERT(TypeFromId(transaction->GetId()) == transaction->GetType());
        return ShouldCacheTransactionPresence(transaction->GetId());
    }

    bool ShouldCacheTransactionPresence(TTransactionId transactionId)
    {
        auto transactionType = TypeFromId(transactionId);
        if (transactionType == EObjectType::UploadTransaction ||
            transactionType == EObjectType::UploadNestedTransaction)
        {
            return false;
        }

        if (CellTagFromId(transactionId) == Bootstrap_->GetCellTag()) {
            return false;
        }

        return true;
    }

    void CacheTransactionStarted(TTransaction* transaction)
    {
        if (ShouldCacheTransactionPresence(transaction)) {
            TransactionPresenceCache_->SetTransactionReplicated(transaction->GetId());
        }
    }

    void CacheTransactionFinished(TTransaction* transaction)
    {
        if (ShouldCacheTransactionPresence(transaction)) {
            TransactionPresenceCache_->SetTransactionRecentlyFinished(transaction->GetId());
        }
    }

    void CacheTransactionFinished(TTransactionId transactionId)
    {
        if (ShouldCacheTransactionPresence(transactionId)) {
            TransactionPresenceCache_->SetTransactionRecentlyFinished(transactionId);
        }
    }

    bool RegisterTransactionLease(
        TTransaction* transaction,
        TCellBase* cell) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!transaction->LeaseCellIds().insert(cell->GetId()).second) {
            return false;
        }

        InsertOrCrash(cell->LeaseTransactionIds(), transaction->GetId());

        auto accountTransactionLease = [&] (TTransaction* transaction) {
            transaction->SetSuccessorTransactionLeaseCount(
                transaction->GetSuccessorTransactionLeaseCount() + 1);
        };
        IteratePredecessorTransactions(transaction, BIND(accountTransactionLease));

        YT_LOG_DEBUG(
            "Transaction lease registered (TransactionId: %v, CellId: %v)",
            transaction->GetId(),
            cell->GetId());

        return true;
    }

    bool UnregisterTransactionLease(
        TTransaction* transaction,
        TCellBase* cell) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!transaction->LeaseCellIds().erase(cell->GetId())) {
            return false;
        }

        EraseOrCrash(cell->LeaseTransactionIds(), transaction->GetId());

        auto discountTransactionLease = [&] (TTransaction* transaction) {
            transaction->SetSuccessorTransactionLeaseCount(
                transaction->GetSuccessorTransactionLeaseCount() - 1);
        };
        IteratePredecessorTransactions(transaction, BIND(discountTransactionLease));

        YT_LOG_DEBUG(
            "Transaction lease unregistered (TransactionId: %v, CellId: %v)",
            transaction->GetId(),
            cell->GetId());

        return true;
    }

    void SaveKeys(NCellMaster::TSaveContext& context)
    {
        TransactionMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context)
    {
        using NYT::Save;

        TransactionMap_.SaveValues(context);
        Save(context, TimestampHolderMap_);
        BoomerangTracker_->Save(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        TransactionMap_.LoadValues(context);
        Load(context, TimestampHolderMap_);
        BoomerangTracker_->Load(context);

        FixExportedObjectsRefs_ = context.GetVersion() < EMasterReign::FixExportedObjectsRefs;
    }


    void OnAfterSnapshotLoaded() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        // Reconstruct NativeTransactions and NativeTopmostTransactions.
        for (auto [id, transaction] : TransactionMap_) {
            if (!IsObjectAlive(transaction)) {
                continue;
            }

            if (transaction->IsNative()) {
                InsertOrCrash(NativeTransactions_, transaction);
                if (!transaction->GetParent()) {
                    InsertOrCrash(NativeTopmostTransactions_, transaction);
                }

                if (FixExportedObjectsRefs_) {
                    if (transaction->GetPersistentState() == ETransactionState::Active) {
                        for (const auto& exportEntry : transaction->ExportedObjects()) {
                            objectManager->RefObject(exportEntry.Object);
                        }
                    }

                    if (!transaction->ExportedObjects().empty() && transaction->GetPersistentState() == ETransactionState::PersistentCommitPrepared) {
                        YT_LOG_ALERT("Found exported objects for transaction in PersistentCommitPrepared state (TransactionId: %v)",
                            transaction->GetId());
                        for (const auto& exportEntry : transaction->ExportedObjects()) {
                            objectManager->RefObject(exportEntry.Object);
                        }
                    }
                }
            } else {
                InsertOrCrash(ForeignTransactions_, transaction);
            }
        }

        // Fill transaction presence cache.
        for (auto [id, transaction] : TransactionMap_) {
            if (IsObjectAlive(transaction)) {
                CacheTransactionStarted(transaction);
            }
        }

        if (IsSequoiaTxBarrierEnabled()) {
            const auto& txSupervisor = Bootstrap_->GetTransactionSupervisor();
            txSupervisor->ClearSequoiaTxRegistry();
            for (auto [id, transaction] : TransactionMap_) {
                if (transaction->IsSequoiaTransaction() &&
                    transaction->GetPersistentState() == ETransactionState::PersistentCommitPrepared)
                {
                    txSupervisor->RegisterPreparedSequoiaTx(id);
                }
            }
        }
    }

    bool IsSequoiaTxBarrierEnabled() const noexcept
    {
        return Bootstrap_->GetConfig()->TransactionSupervisor->EnableWaitUntilPreparedTransactionsFinished;
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TransactionMap_.Clear();
        ForeignTransactions_.clear();
        NativeTopmostTransactions_.clear();
        NativeTransactions_.clear();
        TransactionPresenceCache_->Clear();
        FixExportedObjectsRefs_ = false;
    }

    void OnStartLeading() override
    {
        TMasterAutomatonPart::OnStartLeading();

        OnStartEpoch();
    }

    void OnStartFollowing() override
    {
        TMasterAutomatonPart::OnStartFollowing();

        OnStartEpoch();
    }

    void OnStartEpoch()
    {
        TransactionPresenceCache_->Start();
    }

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        LeaseTracker_->Start();

        // Recreate leases for all active transactions.
        for (auto [transactionId, transaction] : TransactionMap_) {
            auto state = transaction->GetTransientState();
            if (state == ETransactionState::Active ||
                state == ETransactionState::PersistentCommitPrepared)
            {
                CreateLease(transaction);
            }
        }

        BoomerangTracker_->Start();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        BoomerangTracker_->Stop();
        LeaseTracker_->Stop();

        // Reset all transiently prepared transactions back into active state.
        for (auto [transactionId, transaction] : TransactionMap_) {
            transaction->ResetTransientState();

            if (transaction->LeasesRevokedPromise()) {
                auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
                transaction->LeasesRevokedPromise().TrySet(error);
                transaction->LeasesRevokedPromise() = NewPromise<void>();
            }
        }

        OnStopEpoch();
    }

    void OnStopFollowing() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();

        OnStopEpoch();
    }

    void OnStopEpoch()
    {
        TransactionPresenceCache_->Stop();
    }

    void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        BufferedProducer_->SetEnabled(false);
    }

    void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        BufferedProducer_->SetEnabled(true);
    }

    void CreateLease(TTransaction* transaction)
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        LeaseTracker_->RegisterTransaction(
            transaction->GetId(),
            GetObjectId(transaction->GetParent()),
            transaction->GetTimeout(),
            transaction->GetDeadline(),
            BIND(&TTransactionManager::OnTransactionExpired, MakeStrong(this))
                .Via(hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor)));
    }

    void CloseLease(TTransaction* transaction)
    {
        LeaseTracker_->UnregisterTransaction(transaction->GetId());
    }

    void OnTransactionExpired(TTransactionId transactionId)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            return;
        }
        if (transaction->GetTransientState() != ETransactionState::Active) {
            return;
        }

        TFuture<void> abortFuture;
        if (transaction->GetIsCypressTransaction()) {
            TFuture<TSharedRefArray> response;
            if (IsMirroringToSequoiaEnabled() &&
                IsMirroredToSequoia(transaction->GetId()))
            {
                response = AbortExpiredCypressTransactionInSequoia(
                    Bootstrap_,
                    transactionId);
            } else {
                NProto::TReqAbortCypressTransaction request;
                ToProto(request.mutable_transaction_id(), transactionId);
                request.set_force(false);
                WriteAuthenticationIdentityToProto(&request, NRpc::GetRootAuthenticationIdentity());

                auto mutation = CreateMutation(HydraManager_, request);
                response = DoAbortTransaction(std::move(mutation), transaction, /*force*/ false);
            }
            abortFuture = response
                .Apply(BIND([=] (const TErrorOr<TSharedRefArray>& rspOrError) {
                    if (!rspOrError.IsOK()) {
                        return MakeFuture<void>(rspOrError);
                    }

                    const auto& rsp = rspOrError.Value();

                    NRpc::NProto::TResponseHeader header;
                    YT_VERIFY(NRpc::TryParseResponseHeader(rsp, &header));
                    if (header.has_error()) {
                        auto error = FromProto<TError>(header.error());
                        return MakeFuture<void>(error);
                    }

                    return VoidFuture;
                }));
        } else {
            const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
            abortFuture = transactionSupervisor->AbortTransaction(transactionId);
        }

        abortFuture
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError) {
                    if (IsLeader()) {
                        const auto& hydraFacade = Bootstrap_->GetHydraFacade();

                        // Poor man's retry.
                        // TODO(kvk1920): implement transaction abort tracker
                        // and use it here.
                        // NB: We don't need parent id here since it is used
                        // only to ping ancestors.
                        LeaseTracker_->UnregisterTransaction(transactionId);
                        LeaseTracker_->RegisterTransaction(
                            transactionId,
                            /* parentId*/ {},
                            /*timeout*/ TDuration::Zero(),
                            /*deadline*/ std::nullopt,
                            BIND(&TTransactionManager::OnTransactionExpired, MakeStrong(this))
                                .Via(hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor)));
                    }
                } else if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                        transactionId);
                }
            }));
    }

    void IterateSuccessorTransactions(
        TTransaction* transaction,
        TCallback<void(TTransaction*)> callback)
    {
        TCompactSet<TTransaction*, 16> visitedTransactions;
        TCompactQueue<TTransaction*, 16> queue;

        auto tryEnqueue = [&] (TTransaction* transaction) {
            if (visitedTransactions.insert(transaction).second) {
                queue.Push(transaction);
            }
        };

        tryEnqueue(transaction);
        while (!queue.Empty()) {
            auto* currentTransaction = queue.Pop();
            callback(currentTransaction);

            for (auto* nextTransaction : currentTransaction->NestedTransactions()) {
                tryEnqueue(nextTransaction);
            }
            TCompactVector<TTransaction*, 16> dependentTransactions(
                currentTransaction->DependentTransactions().begin(),
                currentTransaction->DependentTransactions().end());
            std::sort(
                dependentTransactions.begin(),
                dependentTransactions.end(),
                TObjectIdComparer());
            for (auto* nextTransaction : dependentTransactions) {
                tryEnqueue(nextTransaction);
            }
        }
    }

    void IteratePredecessorTransactions(
        TTransaction* transaction,
        TCallback<void(TTransaction*)> callback)
    {
        TCompactSet<TTransaction*, 16> visitedTransactions;
        TCompactQueue<TTransaction*, 16> queue;

        auto tryEnqueue = [&] (TTransaction* transaction) {
            if (visitedTransactions.insert(transaction).second) {
                queue.Push(transaction);
            }
        };

        tryEnqueue(transaction);
        while (!queue.Empty()) {
            auto* currentTransaction = queue.Pop();
            callback(currentTransaction);

            if (auto* nextTransaction = currentTransaction->GetParent()) {
                tryEnqueue(nextTransaction);
            }

            TCompactVector<TTransaction*, 16> prerequisiteTransactions(
                currentTransaction->PrerequisiteTransactions().begin(),
                currentTransaction->PrerequisiteTransactions().end());
            std::sort(
                prerequisiteTransactions.begin(),
                prerequisiteTransactions.end(),
                TObjectIdComparer());
            for (auto* nextTransaction : prerequisiteTransactions) {
                tryEnqueue(nextTransaction);
            }
        }
    }

    TFuture<void> RevokeTransactionLeases(TTransactionId transactionId)
    {
        NProto::TReqRevokeLeases request;
        ToProto(request.mutable_transaction_id(), transactionId);
        auto mutation = CreateMutation(HydraManager_, request);
        return mutation->Commit().AsVoid().Apply(BIND([=, this, this_ = MakeStrong(this)] {
            YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

            auto* transaction = FindTransaction(transactionId);
            // Transaction was already committed or aborted. Let commit and abort handler
            // deal with it.
            if (!transaction) {
                return VoidFuture;
            }

            if (transaction->GetTransactionLeasesState() == ETransactionLeasesState::Active) {
                YT_LOG_ALERT(
                    "Transaction has unexpected leases state after lease revocation "
                    "(TransactionId: %v, LeasesState: %v)",
                    transaction->GetId(),
                    transaction->GetTransactionLeasesState());
                return VoidFuture;
            }

            if (GetDynamicConfig()->ThrowOnLeaseRevocation) {
                auto error = TError("Testing error");
                return MakeFuture<void>(error);
            }

            auto leaseRevocationFuture = transaction->LeasesRevokedPromise().ToFuture();
            return leaseRevocationFuture.ToUncancelable();
        }).AsyncVia(EpochAutomatonInvoker_));
    }

    void OnLeaseRevoked(TLeaseId leaseId, TCellId cellId)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto* transaction = FindTransaction(leaseId);
        if (!transaction) {
            YT_LOG_DEBUG(
                "Unknown lease was revoked, ignored (LeaseId: %v, CellId: %v)",
                leaseId,
                cellId);
            return;
        }

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCell(cellId);
        if (!cell) {
            YT_LOG_DEBUG(
                "Lease was revoked on unknown cell, ignored (LeaseId: %v, CellId: %v)",
                leaseId,
                cellId);
            return;
        }

        if (!UnregisterTransactionLease(transaction, cell)) {
            YT_LOG_DEBUG(
                "Unregistered lease was revoked, ignored (LeaseId: %v, CellId: %v)",
                leaseId,
                cellId);
        }
    }

    TSequoiaContextGuard MaybeCreateSequoiaContextGuard(TTransaction* transaction)
    {
        if (transaction->IsSequoiaTransaction()) {
            auto sequoiaContext = CreateSequoiaContext(Bootstrap_, transaction->GetId(), transaction->SequoiaWriteSet());
            return TSequoiaContextGuard(std::move(sequoiaContext), Bootstrap_->GetSecurityManager(), transaction->GetAuthenticationIdentity(), transaction->GetTraceContext());
        } else {
            return TSequoiaContextGuard(Bootstrap_->GetSecurityManager());
        }
    }

    void OnProfiling()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (TransactionPresenceCache_) {
            TSensorBuffer buffer;

            buffer.AddGauge("/cached_replicated_transaction_count", TransactionPresenceCache_->GetReplicatedTransactionCount());
            buffer.AddGauge("/cached_recently_finished_transaction_count", TransactionPresenceCache_->GetRecentlyFinishedTransactionCount());
            buffer.AddGauge("/subscribed_remote_transaction_replication_count", TransactionPresenceCache_->GetSubscribedRemoteTransactionReplicationCount());

            BufferedProducer_->Update(std::move(buffer));
        }
    }

    const TDynamicTransactionManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        ProfilingExecutor_->SetPeriod(GetDynamicConfig()->ProfilingPeriod);
    }

    void ThrowTransactionSuccessorHasLeases(TTransaction* transaction)
    {
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::TransactionSuccessorHasLeases,
            "Transaction successor has leases issued")
            << TErrorAttribute("transaction_id", transaction->GetId())
            << TErrorAttribute("successor_transaction_lease_count", transaction->GetSuccessorTransactionLeaseCount());
    }

    std::unique_ptr<NHydra::TMutation> CreateStartCypressTransactionMutation(
        TCtxStartCypressTransactionPtr context,
        const NProto::TReqStartCypressTransaction& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            request,
            &TTransactionManager::HydraStartCypressTransaction,
            this);
    }

    bool IsMirroringToSequoiaEnabled()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->SequoiaManager;
        return config->Enable && config->EnableCypressTransactionsInSequoia;
    }

    // NB: This function doesn't work properly if Cypress transaction service is
    // not used.
    static bool IsMirroredToSequoia(TTransactionId transactionId)
    {
        return IsCypressTransactionType(TypeFromId(transactionId)) && IsSequoiaId(transactionId);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TransactionMap_);

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(TBootstrap* bootstrap)
{
    return New<TTransactionManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
