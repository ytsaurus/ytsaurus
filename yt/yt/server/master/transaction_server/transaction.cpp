#include "transaction.h"
#include "private.h"
#include "transaction_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/account_resource_usage_lease.h>
#include <yt/yt/server/master/security_server/subject.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

int TBranchedNodeSet::Size() const noexcept
{
    return std::ssize(Nodes_);
}

bool TBranchedNodeSet::Empty() const noexcept
{
    return Nodes_.empty();
}

void TBranchedNodeSet::Clear()
{
    Nodes_.clear();
    NodeToIndex_.clear();
}

TBranchedNodeSet::TIterator TBranchedNodeSet::begin() const noexcept
{
    return Nodes_.begin();
}

TBranchedNodeSet::TIterator TBranchedNodeSet::end() const noexcept
{
    return Nodes_.end();
}

TCypressNode* TBranchedNodeSet::GetAnyNode()
{
    YT_ASSERT(!Nodes_.empty());
    return Nodes_.back();
}

void TBranchedNodeSet::InsertOrCrash(TCypressNode* node)
{
    NYT::EmplaceOrCrash(NodeToIndex_, node, Nodes_.size());
    Nodes_.push_back(node);
}

void TBranchedNodeSet::EraseOrCrash(TCypressNode* node)
{
    auto it = NodeToIndex_.find(node);
    YT_VERIFY(it != NodeToIndex_.end());

    auto index = it->second;
    YT_VERIFY(index < std::ssize(Nodes_));

    if (index < std::ssize(Nodes_) - 1) {
        // Move last element to the place of removed element.
        GetOrCrash(NodeToIndex_, Nodes_.back()) = index;
        std::swap(Nodes_[index], Nodes_.back());
    }

    Nodes_.pop_back();
    NodeToIndex_.erase(it);
}

bool TBranchedNodeSet::Contains(TCypressNode* node) const
{
    return NodeToIndex_.contains(node);
}

void TBranchedNodeSet::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Nodes_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->NodeToIndex_.clear();
        this_->NodeToIndex_.reserve(this_->Nodes_.size());
        for (int index = 0; index < std::ssize(this_->Nodes_); ++index) {
            EmplaceOrCrash(this_->NodeToIndex_, this_->Nodes_[index], index);
        }
    });
}

PHOENIX_DEFINE_TYPE(TBranchedNodeSet);

////////////////////////////////////////////////////////////////////////////////

TBulkInsertState::TBulkInsertState(TTransaction* transaction)
    : Transaction_(transaction)
{
    YT_VERIFY(Transaction_);
}

void TBulkInsertState::OnTransactionCommitted(bool isTimestampPresentInHolder)
{
    YT_VERIFY(Transaction_);
    FindTopmostTransaction();

    if (auto parentTransaction = Transaction_->GetParent()) {
        auto& parentBulkInsertState = parentTransaction->BulkInsertState();

        if (isTimestampPresentInHolder) {
            parentBulkInsertState.DescendantTransactionsPresentedInTimestampHolder_.push_back(Transaction_->GetId());
        }

        parentBulkInsertState.DescendantTransactionsPresentedInTimestampHolder_.insert(
            parentBulkInsertState.DescendantTransactionsPresentedInTimestampHolder_.end(),
            DescendantTransactionsPresentedInTimestampHolder_.begin(),
            DescendantTransactionsPresentedInTimestampHolder_.end());

        parentBulkInsertState.AddToLockableDynamicTables(std::move(LockableDynamicTables_));
    }
}

void TBulkInsertState::OnTransactionAborted()
{
    YT_VERIFY(Transaction_);
    FindTopmostTransaction();

    if (TopmostTransaction_ != Transaction_) {
        for (const auto& [externalCellTag, tableIds] : LockableDynamicTables_) {
            for (auto tableId : tableIds) {
                EraseOrCrash(TopmostTransaction_->BulkInsertState().AllTablesLockedByDescendantTransactions_, tableId);
            }
        }
    }
}

bool TBulkInsertState::HasConflict(TTableId tableId)
{
    YT_VERIFY(Transaction_);
    FindTopmostTransaction();

    return TopmostTransaction_->BulkInsertState().AllTablesLockedByDescendantTransactions_.contains(tableId);
}

void TBulkInsertState::AddLockableDynamicTables(
    std::vector<std::pair<TCellTag, std::vector<TTableId>>>&& lockableDynamicTables)
{
    YT_VERIFY(Transaction_);
    FindTopmostTransaction();

    auto& conflictResolver = TopmostTransaction_->BulkInsertState().AllTablesLockedByDescendantTransactions_;
    for (const auto& [externalCellTag, tableIds] : lockableDynamicTables) {
        conflictResolver.insert(tableIds.begin(), tableIds.end());
    }

    AddToLockableDynamicTables(std::move(lockableDynamicTables));
}

void TBulkInsertState::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, LockableDynamicTables_);
    PHOENIX_REGISTER_FIELD(2, LockedDynamicTables_);
    PHOENIX_REGISTER_FIELD(3, AllTablesLockedByDescendantTransactions_);
    PHOENIX_REGISTER_FIELD(4, DescendantTransactionsPresentedInTimestampHolder_);
}

PHOENIX_DEFINE_TYPE(TBulkInsertState);

void TBulkInsertState::FindTopmostTransaction()
{
    if (TopmostTransaction_ == nullptr) {
        TopmostTransaction_ = Transaction_->GetTopmostTransaction();
    }
}

template <class TLockableDynamicTables>
void TBulkInsertState::AddToLockableDynamicTables(TLockableDynamicTables&& lockableDynamicTables)
{
    for (auto&& externalCellTagToTableIds : lockableDynamicTables) {
        const auto& [externalCellTag, tableIds] = externalCellTagToTableIds;

        TCellTagToLockableTables::insert_ctx mapInsertContext;
        if (auto it = LockableDynamicTables_.find(externalCellTag, mapInsertContext);
            it == LockableDynamicTables_.end())
        {
            LockableDynamicTables_.insert_direct(std::move(externalCellTagToTableIds), mapInsertContext);
        } else {
            std::ranges::move(tableIds, std::back_inserter(it->second));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTransaction::TExportEntry::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Object);
    PHOENIX_REGISTER_FIELD(2, DestinationCellTag);
}

PHOENIX_DEFINE_TYPE(TTransaction::TExportEntry);

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TTransactionId id)
    : TTransactionBase(id)
    , StartTime_(TInstant::Zero())
    , Acd_(this)
{ }

std::string TTransaction::GetLowercaseObjectName() const
{
    return Format("transaction %v", GetId());
}

std::string TTransaction::GetCapitalizedObjectName() const
{
    return Format("Transaction %v", GetId());
}

TYPath TTransaction::GetObjectPath() const
{
    return Format("//sys/transactions/%v", GetId());
}

void TTransaction::RegisterMetadata(auto&& registrar)
{
    // NB: TTransactionBase is not a Phoenix type; TObject and TTransactionBase
    // parts are persisted via this leading virtual field.
    registrar.template VirtualField<1>("Base", [] (TThis* this_, auto& context) {
        this_->TObject::Load(context);
        this_->TTransactionBase::Load(context);
    }, [] (const TThis* this_, auto& context) {
        this_->TObject::Save(context);
        this_->TTransactionBase::Save(context);

        // All transactions should have this flag set to true, unless they were started before 24.2
        // and something went wrong when attempting to abort it.
        // But let's write a detailed message here anyway.
        YT_TLOG_ALERT_UNLESS(
            this_->NativeTxExternalizationEnabled_,
            "A Cypress transaction has NativeTxExternalizationEnabled set to false; "
            "this can happen if the transaction was started too long ago; the new algorithm "
            "relies on all transactions being externalized, so this may lead to data corruption; "
            "please abort said transaction")
            .With("TransactionId", this_->Id_);
    })();

    registrar.template PropertyField<2, &TThis::GetPersistentState, &TThis::SetPersistentState>("PersistentState")();

    PHOENIX_REGISTER_FIELD(3, Timeout_);
    PHOENIX_REGISTER_FIELD(4, Title_);
    PHOENIX_REGISTER_FIELD(5, ReplicatedToCellTags_);
    PHOENIX_REGISTER_FIELD(6, ExternalizedToCellTags_);
    PHOENIX_REGISTER_FIELD(7, NestedTransactions_);
    PHOENIX_REGISTER_FIELD(8, Parent_);
    PHOENIX_REGISTER_FIELD(9, StartTime_);
    PHOENIX_REGISTER_FIELD(10, StagedObjects_);
    PHOENIX_REGISTER_FIELD(11, ExportedObjects_);
    PHOENIX_REGISTER_FIELD(12, ImportedObjects_);
    PHOENIX_REGISTER_FIELD(13, LockedNodes_);
    PHOENIX_REGISTER_FIELD(14, Locks_);
    PHOENIX_REGISTER_FIELD(15, BranchedNodes_);

    // COMPAT(theevilbird): Load-only.
    registrar.template VirtualField<16>("StagedNodes_", [] (TThis* this_, auto& context) {
        NYT::Load(context, this_->StagedNodes_);
    })
        .InVersions([] (EMasterReign version) {
            return
                version < EMasterReign::RemoveStagedNodesInTransactions ||
                (version >= EMasterReign::Start_26_2 && version < EMasterReign::RemoveStagedNodesInTransactions_26_2);
        })();

    PHOENIX_REGISTER_FIELD(17, AccountResourceUsage_);
    PHOENIX_REGISTER_FIELD(18, Acd_);
    PHOENIX_REGISTER_FIELD(19, PrerequisiteTransactions_);
    PHOENIX_REGISTER_FIELD(20, DependentTransactions_);
    PHOENIX_REGISTER_FIELD(21, Deadline_);
    PHOENIX_REGISTER_FIELD(22, BulkInsertState_);
    PHOENIX_REGISTER_FIELD(23, TablesWithBackupCheckpoints_);
    PHOENIX_REGISTER_FIELD(24, Depth_);
    PHOENIX_REGISTER_DELETED_FIELD(25, bool, Upload_, EMasterReign::RemoveCompatsAroundStartTransaction);
    PHOENIX_REGISTER_FIELD(26, RecursiveLockCount_);

    registrar.template PropertyField<27,
        [] (const TThis* this_) {
            return this_->NativeCommitRevision_.Mutation;
        },
        [] (TThis* this_, NHydra::TRevision revision) {
            this_->NativeCommitRevision_.Mutation = revision;
        }>("NativeCommitRevision_.Mutation")();

    // Yes, this heavily relies on timestamps being significantly greater than
    // every possible Hydra revision.
    registrar.template PropertyField<28,
        [] (const TThis* this_) {
            return this_->NativeCommitRevision_.Sequoia;
        },
        [] (TThis* this_, NHydra::TRevision revision) {
            this_->NativeCommitRevision_.Sequoia = revision;
        }>("NativeCommitRevision_.Sequoia")
        .InVersions([] (EMasterReign version) {
            return !(
                version < EMasterReign::SequoiaRevision_26_1 ||
                (version >= EMasterReign::Start_26_2 && version < EMasterReign::SequoiaRevision_26_2));
        })
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->NativeCommitRevision_.Sequoia = this_->NativeCommitRevision_.Mutation;
        })();

    PHOENIX_REGISTER_DELETED_FIELD(29, bool, IsCypressTransaction_, EMasterReign::RemoveCompatsAroundStartTransaction);

    // NB: Leases state is applied via SetTransactionLeasesState once SuccessorTransactionLeaseCount_ is loaded.
    registrar.template PropertyField<30,
        &TThis::GetTransactionLeasesState,
        [] (TThis* this_, ETransactionLeasesState state) {
            this_->LeasesState_ = state;
        }>("LeasesState_")();

    PHOENIX_REGISTER_FIELD(31, LeaseCellIds_);

    registrar.template VirtualField<32>("SuccessorTransactionLeaseCount_", [] (TThis* this_, auto& context) {
        NYT::Load(context, this_->SuccessorTransactionLeaseCount_);
        this_->SetTransactionLeasesState(this_->LeasesState_);
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->SuccessorTransactionLeaseCount_);
    })();

    PHOENIX_REGISTER_FIELD(33, AccountResourceUsageLeases_);
    PHOENIX_REGISTER_FIELD(34, SequoiaTransaction_);
    PHOENIX_REGISTER_FIELD(35, SequoiaWriteSet_);

    PHOENIX_REGISTER_FIELD(36, AuthenticationIdentity_);

    // COMPAT(ivpiskarev)
    PHOENIX_REGISTER_FIELD(38, InitiatorId_,
        .SinceVersion(EMasterReign::AddPerUserActiveTransactionCountLimit)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            // We are supposed to be in LoadValues, so all objects should have been
            // allocated and should have correct ids.
            this_->InitiatorId_ = this_->Acd_.GetOwner()->AsUser()->GetId();
        }));

    // COMPAT(h0pless): AbortStuckTransactions.
    registrar.template VirtualField<39>("NativeTxExternalizationEnabled_", [] (TThis* this_, auto& context) {
        if (IsCypressTransactionType(this_->GetType())) {
            NYT::Load(context, this_->NativeTxExternalizationEnabled_);
        }
    }, [] (const TThis* this_, auto& context) {
        if (IsCypressTransactionType(this_->GetType())) {
            NYT::Save(context, this_->NativeTxExternalizationEnabled_);
        }
    })
        .InVersions([] (EMasterReign version) {
            return
                version < EMasterReign::RemoveNativeTxExternalizationEnabledFlag ||
                (EMasterReign::AbortStuckTransactions <= version && version < EMasterReign::Start_26_2) ||
                EMasterReign::AbortStuckTransactions_26_2 <= version;
        })();

    // NB: Cookies are invalidated anyway, so let's save the keys only.
    registrar.template VirtualField<40>("BarrierTags", [] (TThis* this_, auto& context) {
        auto barrierTags = NYT::Load<std::vector<std::string>>(context);
        for (const auto& tag : barrierTags) {
            this_->RegisterBarrierCookie(tag, InvalidAsyncBarrierCookie);
        }
    }, [] (const TThis* this_, auto& context) {
        auto tags = GetKeys(this_->BarrierTagToCookie_);
        std::sort(tags.begin(), tags.end());
        NYT::Save(context, tags);
    })
        .InVersions([] (EMasterReign version) {
            return
                (EMasterReign::StrongOrderingTags <= version && version < EMasterReign::Start_26_2) ||
                EMasterReign::StrongOrderingTags_26_2 <= version;
        })();
}

PHOENIX_DEFINE_TYPE(TTransaction);

const TTransaction* TTransaction::GetTopmostTransaction() const
{
    auto* currentTransaction = this;
    while (currentTransaction->GetParent()) {
        currentTransaction = currentTransaction->GetParent();
    }
    return currentTransaction;
}

TTransaction* TTransaction::GetTopmostTransaction()
{
    return const_cast<TTransaction*>(const_cast<const TTransaction*>(this)->GetTopmostTransaction());
}

bool TTransaction::IsReplicatedToCell(TCellTag cellTag) const
{
    return ReplicatedToCellTags_.contains(cellTag);
}

bool TTransaction::IsExternalizedToCell(TCellTag cellTag) const
{
    return ExternalizedToCellTags_.contains(cellTag);
}

bool TTransaction::IsExternalized() const
{
    return IsExternalizedTransactionType(GetType());
}

bool TTransaction::IsCypressTransaction() const
{
    return IsCypressTransactionType(GetType());
}

bool TTransaction::IsUpload() const
{
    return IsUploadTransactionType(GetType());
}

TTransactionId TTransaction::GetOriginalTransactionId() const
{
    YT_VERIFY(IsExternalized());
    return NTransactionClient::OriginalFromExternalizedTransactionId(Id_);
}

int TTransaction::GetRecursiveLockCount() const
{
    return RecursiveLockCount_;
}

void TTransaction::AttachLock(TLock* lock, const IObjectManagerPtr& objectManager)
{
    YT_VERIFY(!IsSequoiaTransaction());

    EmplaceOrCrash(Locks_, lock);
    lock->SetTransaction(this);
    objectManager->RefObject(lock);

    auto* currentTransaction = this;
    while (currentTransaction) {
        currentTransaction->IncrementRecursiveLockCount();

        YT_TLOG_TRACE("Transaction recursive lock count increased")
            .With("RecursiveLockCount", currentTransaction->GetRecursiveLockCount())
            .With("TransactionId", currentTransaction->GetId())
            .With("LockId", lock->GetId());

        currentTransaction = currentTransaction->GetParent();
    }
}

void TTransaction::DetachLock(
    TLock* lock,
    const IObjectManagerPtr& objectManager,
    bool resetLockTransaction)
{
    YT_VERIFY(!IsSequoiaTransaction());

    EraseOrCrash(Locks_, lock);
    if (resetLockTransaction) {
        lock->SetTransaction(nullptr);
    }
    objectManager->UnrefObject(lock);

    auto* currentTransaction = this;
    while (currentTransaction) {
        currentTransaction->DecrementRecursiveLockCount();

        YT_TLOG_TRACE("Transaction recursive lock count decreased")
            .With("RecursiveLockCount", currentTransaction->GetRecursiveLockCount())
            .With("TransactionId", currentTransaction->GetId())
            .With("LockId", lock->GetId());

        currentTransaction = currentTransaction->GetParent();
    }
}

void TTransaction::IncrementRecursiveLockCount()
{
    ++RecursiveLockCount_;
}

void TTransaction::DecrementRecursiveLockCount()
{
    YT_TLOG_ALERT_IF(--RecursiveLockCount_ < 0, "Transaction recursive lock count is negative")
        .With("TransactionId", GetId())
        .With("RecursiveLockCount", RecursiveLockCount_);
}

void TTransaction::SetTransactionLeasesState(ETransactionLeasesState newState)
{
    LeasesState_ = newState;

    if (newState == ETransactionLeasesState::Revoking) {
        if (!LeasesRevokedPromise_) {
            LeasesRevokedPromise_ = NewPromise<void>();
        }
        if (SuccessorTransactionLeaseCount_ == 0) {
            SetTransactionLeasesState(ETransactionLeasesState::Revoked);
        }
    } else if (newState == ETransactionLeasesState::Revoked) {
        // May happen during snapshot load.
        if (!LeasesRevokedPromise_) {
            LeasesRevokedPromise_ = NewPromise<void>();
        }
        LeasesRevokedPromise_.TrySet();
        YT_TLOG_DEBUG("Transaction leases revoked")
            .With("TransactionId", Id_);
    }
}

ETransactionLeasesState TTransaction::GetTransactionLeasesState() const
{
    return LeasesState_;
}

void TTransaction::SetSuccessorTransactionLeaseCount(int newLeaseCount)
{
    SuccessorTransactionLeaseCount_ = newLeaseCount;
    if (SuccessorTransactionLeaseCount_ == 0 &&
        LeasesState_ == ETransactionLeasesState::Revoking)
    {
        SetTransactionLeasesState(ETransactionLeasesState::Revoked);
    }
}

int TTransaction::GetSuccessorTransactionLeaseCount() const
{
    return SuccessorTransactionLeaseCount_;
}

bool TTransaction::IsNativeTxExternalizationEnabled() const
{
    return NativeTxExternalizationEnabled_;
}

void TTransaction::RegisterBarrierCookie(
    const std::string& tag,
    TAsyncBarrierCookie cookie)
{
    if (cookie == InvalidAsyncBarrierCookie) {
        YT_VERIFY(!BarrierTagToCookie_.contains(tag));
    }

    BarrierTagToCookie_[tag] = cookie;
}

const THashMap<std::string, TAsyncBarrierCookie>& TTransaction::GetBarrierCookies() const
{
    return BarrierTagToCookie_;
}

auto TTransaction::GetActionStateFactory() -> IActionStateFactory*
{
    return GetBootstrap()
        ->GetTransactionManager()
        ->GetTransactionActionStateFactory();
}

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TFluent>
void DumpTransaction(TFluent fluent, const TTransaction* transaction, bool dumpParents)
{
    auto customAttributes = CreateEphemeralAttributes();
    auto copyCustomAttribute = [&] (const std::string& key) {
        if (!transaction->GetAttributes()) {
            return;
        }
        const auto& attributeMap = transaction->GetAttributes()->Attributes();
        auto it = attributeMap.find(key);
        if (it == attributeMap.end()) {
            return;
        }
        customAttributes->SetYson(it->first, it->second);
    };
    copyCustomAttribute("operation_id");
    copyCustomAttribute("operation_title");

    fluent
        .BeginMap()
            .Item("id").Value(transaction->GetId())
            .Item("start_time").Value(transaction->GetStartTime())
            .Item("owner").Value(transaction->Acd().GetOwner()->GetName())
            .OptionalItem("timeout", transaction->GetTimeout())
            .OptionalItem("title", transaction->GetTitle())
            .DoIf(dumpParents, [&] (auto fluent) {
                std::vector<TTransaction*> parents;
                auto parent = transaction->GetParent();
                while (parent) {
                    parents.push_back(parent);
                    parent = parent->GetParent();
                }
                fluent.Item("parents").DoListFor(parents, [&] (auto fluent, auto* parent) {
                    fluent
                        .Item().Do([&] (auto fluent) {
                            DumpTransaction(fluent, parent, false);
                        });
                });
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TYsonString TTransaction::GetErrorDescription() const
{
    return BuildYsonStringFluently()
        .Do([&] (auto fluent) {
            DumpTransaction(fluent, this, true);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
