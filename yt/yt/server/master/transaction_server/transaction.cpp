#include "transaction.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/account_resource_usage_lease.h>
#include <yt/yt/server/master/security_server/subject.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TransactionServerLogger;

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

NCypressServer::TCypressNode* TBranchedNodeSet::GetAnyNode()
{
    return Nodes_.back();
}

void TBranchedNodeSet::InsertOrCrash(NCypressServer::TCypressNode* node)
{
    NYT::EmplaceOrCrash(NodeToIndex_, node, Nodes_.size());
    Nodes_.push_back(node);
}

void TBranchedNodeSet::EraseOrCrash(NCypressServer::TCypressNode* node)
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

void TBranchedNodeSet::Persist(const NCellMaster::TPersistenceContext& context)
{
    NYT::Persist(context, Nodes_);
    if (context.IsLoad()) {
        NodeToIndex_.clear();
        NodeToIndex_.reserve(Nodes_.size());
        for (int index = 0; index < std::ssize(Nodes_); ++index) {
            EmplaceOrCrash(NodeToIndex_, Nodes_[index], index);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTransaction::TExportEntry::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Object);
    Persist(context, DestinationCellTag);
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TTransactionId id, bool upload)
    : TTransactionBase(id)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
    , Acd_(this)
    , Upload_(upload)
{ }

bool TTransaction::IsUpload() const
{
    return Upload_;
}

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

void TTransaction::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);
    TTransactionBase::Save(context);

    using NYT::Save;
    Save(context, GetPersistentState());
    Save(context, Timeout_);
    Save(context, Title_);
    Save(context, ReplicatedToCellTags_);
    Save(context, ExternalizedToCellTags_);
    Save(context, NestedTransactions_);
    Save(context, Parent_);
    Save(context, StartTime_);
    Save(context, StagedObjects_);
    Save(context, ExportedObjects_);
    Save(context, ImportedObjects_);
    Save(context, LockedNodes_);
    Save(context, Locks_);
    Save(context, BranchedNodes_);
    Save(context, StagedNodes_);
    Save(context, AccountResourceUsage_);
    Save(context, Acd_);
    Save(context, PrerequisiteTransactions_);
    Save(context, DependentTransactions_);
    Save(context, Deadline_);
    Save(context, LockedDynamicTables_);
    Save(context, TablesWithBackupCheckpoints_);
    Save(context, Depth_);
    Save(context, Upload_);
    Save(context, RecursiveLockCount_);
    Save(context, NativeCommitMutationRevision_);
    Save(context, IsCypressTransaction_);
    Save(context, GetTransactionLeasesState());
    Save(context, LeaseCellIds_);
    Save(context, SuccessorTransactionLeaseCount_);
    Save(context, AccountResourceUsageLeases_);
    Save(context, SequoiaTransaction_);
    Save(context, SequoiaWriteSet_);
    Save(context, AuthenticationIdentity_.User);
    Save(context, AuthenticationIdentity_.UserTag);

    if (IsCypressTransactionType(GetType())) {
        Save(context, NativeTxExternalizationEnabled_);
    }
}

void TTransaction::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);
    TTransactionBase::Load(context);

    using NYT::Load;
    SetPersistentState(Load<ETransactionState>(context));
    Load(context, Timeout_);
    Load(context, Title_);
    Load(context, ReplicatedToCellTags_);
    Load(context, ExternalizedToCellTags_);
    Load(context, NestedTransactions_);
    Load(context, Parent_);
    Load(context, StartTime_);
    Load(context, StagedObjects_);
    Load(context, ExportedObjects_);
    Load(context, ImportedObjects_);
    Load(context, LockedNodes_);
    Load(context, Locks_);
    Load(context, BranchedNodes_);
    Load(context, StagedNodes_);
    Load(context, AccountResourceUsage_);
    Load(context, Acd_);
    Load(context, PrerequisiteTransactions_);
    Load(context, DependentTransactions_);
    Load(context, Deadline_);
    Load(context, LockedDynamicTables_);
    Load(context, TablesWithBackupCheckpoints_);
    Load(context, Depth_);
    Load(context, Upload_);
    Load(context, RecursiveLockCount_);
    Load(context, NativeCommitMutationRevision_);
    Load(context, IsCypressTransaction_);

    // COMPAT(gritukan)
    if (context.GetVersion() >= EMasterReign::TabletPrerequisites) {
        SetTransactionLeasesState(Load<ETransactionLeasesState>(context));
        Load(context, LeaseCellIds_);
        Load(context, SuccessorTransactionLeaseCount_);
    }

    Load(context, AccountResourceUsageLeases_);
    Load(context, SequoiaTransaction_);
    Load(context, SequoiaWriteSet_);

    // COMPAT(kvk1920)
    if (context.GetVersion() >= EMasterReign::SaneTxActionAbort &&
        context.GetVersion() < EMasterReign::SaneTxActionAbortFix)
    {
        Load(context, PreparedActionCount_);
    }

    // COMPAT(kvk1920)
    if (context.GetVersion() >= EMasterReign::FixCypressTransactionMirroring ||
        (context.GetVersion() >= EMasterReign::FixCypressTransactionMirroring_24_1 &&
         context.GetVersion() < EMasterReign::DropLegacyClusterNodeMap))
    {
        Load(context, AuthenticationIdentity_.User);
        Load(context, AuthenticationIdentity_.UserTag);
    }

    if (IsCypressTransactionType(GetType())) {
        // COMPAT(kvk1920)
        if (context.GetVersion() >= EMasterReign::NativeTransactionExternalization) {
            Load(context, NativeTxExternalizationEnabled_);
        } else {
            NativeTxExternalizationEnabled_ = false;
        }
    }
}

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
    return std::find(ReplicatedToCellTags_.begin(), ReplicatedToCellTags_.end(), cellTag) != ReplicatedToCellTags_.end();
}

bool TTransaction::IsExternalizedToCell(TCellTag cellTag) const
{
    return std::find(ExternalizedToCellTags_.begin(), ExternalizedToCellTags_.end(), cellTag) != ExternalizedToCellTags_.end();
}

bool TTransaction::IsExternalized() const
{
    return GetType() == EObjectType::ExternalizedTransaction ||
           GetType() == EObjectType::ExternalizedNestedTransaction;
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

void TTransaction::AttachLock(NCypressServer::TLock* lock, const IObjectManagerPtr& objectManager)
{
    YT_VERIFY(!IsSequoiaTransaction());

    EmplaceOrCrash(Locks_, lock);
    lock->SetTransaction(this);
    objectManager->RefObject(lock);

    auto* currentTransaction = this;
    while (currentTransaction) {
        currentTransaction->IncrementRecursiveLockCount();

        YT_LOG_TRACE(
            "Transaction recursive lock count increased (RecursiveLockCount: %v, TransactionId: %v, LockId: %v)",
            currentTransaction->GetRecursiveLockCount(),
            currentTransaction->GetId(),
            lock->GetId());

        currentTransaction = currentTransaction->GetParent();
    }
}

void TTransaction::DetachLock(
    NCypressServer::TLock* lock,
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

        YT_LOG_TRACE(
            "Transaction recursive lock count decreased (RecursiveLockCount: %v, TransactionId: %v, LockId: %v)",
            currentTransaction->GetRecursiveLockCount(),
            currentTransaction->GetId(),
            lock->GetId());

        currentTransaction = currentTransaction->GetParent();
    }
}

void TTransaction::IncreaseRecursiveLockCount(int delta)
{
    YT_VERIFY(delta >= 0);
    RecursiveLockCount_ += delta;
}

void TTransaction::IncrementRecursiveLockCount()
{
    ++RecursiveLockCount_;
}

void TTransaction::DecrementRecursiveLockCount()
{
    YT_LOG_ALERT_IF(--RecursiveLockCount_ < 0,
        "Transaction recursive lock count is negative (TransactionId: %v, RecursiveLockCount: %v)",
        GetId(),
        RecursiveLockCount_);
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

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TFluent>
void DumpTransaction(TFluent fluent, const TTransaction* transaction, bool dumpParents)
{
    auto customAttributes = CreateEphemeralAttributes();
    auto copyCustomAttribute = [&] (const TString& key) {
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

