#include "stdafx.h"
#include "node.h"

#include <server/security_server/account.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id)
    : TObjectBase(id.ObjectId)
    , LockMode_(ELockMode::None)
    , TrunkNode_(nullptr)
    , Transaction_(nullptr)
    , CreationTime_(0)
    , ModificationTime_(0)
    , AccessTime_(0)
    , AccessCounter_(0)
    , Revision_(0)
    , Account_(nullptr)
    , CachedResourceUsage_(ZeroClusterResources())
    , Acd_(this)
    , AccessStatisticsUpdate_(nullptr)
    , Parent_(nullptr)
    , TransactionId(id.TransactionId)
{ }

TCypressNodeBase::~TCypressNodeBase()
{ }

TCypressNodeBase* TCypressNodeBase::GetParent() const
{
    return Parent_;
}

void TCypressNodeBase::SetParent(TCypressNodeBase* newParent)
{
    if (Parent_ == newParent)
        return;

    // Drop old parent.
    if (Parent_) {
        YCHECK(Parent_->ImmediateDescendants().erase(this) == 1);
    }

    // Set new parent.
    Parent_ = newParent;
    if (Parent_) {
        YCHECK(Parent_->IsTrunk());
        YCHECK(Parent_->ImmediateDescendants().insert(this).second);
    }
}

void TCypressNodeBase::ResetParent()
{
    Parent_ = nullptr;
}

TVersionedNodeId TCypressNodeBase::GetVersionedId() const
{
    return TVersionedNodeId(Id, TransactionId);
}

void TCypressNodeBase::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    SaveObjectRefs(context, LockStateMap_);
    SaveObjectRefs(context, AcquiredLocks_);
    SaveObjectRefs(context, PendingLocks_);
    // TODO(babenko): refactor when new serialization API is ready
    auto parentId = Parent_ ? Parent_->GetId() : NullObjectId;
    Save(context, parentId);
    Save(context, LockMode_);
    Save(context, CreationTime_);
    Save(context, ModificationTime_);
    Save(context, Revision_);
    SaveObjectRef(context, Account_);
    Save(context, CachedResourceUsage_);
    Save(context, Acd_);
    Save(context, AccessTime_);
    Save(context, AccessCounter_);
}

void TCypressNodeBase::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    LoadObjectRefs(context, LockStateMap_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 25) {
        LoadObjectRefs(context, AcquiredLocks_);
        LoadObjectRefs(context, PendingLocks_);
    } 
    // COMPAT(babenko)
    if (context.GetVersion() == 24) {
        TLockList locks;
        LoadObjectRefs(context, locks);
        FOREACH (auto* lock, locks) {
            switch (lock->GetState()) {
                case ELockState::Acquired:
                    AcquiredLocks_.push_back(lock);
                    break;
                case ELockState::Pending:
                    PendingLocks_.push_back(lock);
                    break;
                default:
                    YUNREACHABLE();
            }
        }
    }
    // TODO(babenko): refactor when new serialization API is ready
    TNodeId parentId;
    Load(context, parentId);
    Parent_ = parentId == NullObjectId ? nullptr : context.Get<TCypressNodeBase>(parentId);
    Load(context, LockMode_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 22) {
        Load(context, Revision_);
    }
    LoadObjectRef(context, Account_);
    Load(context, CachedResourceUsage_);
    Load(context, Acd_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 23) {
        Load(context, AccessTime_);
        Load(context, AccessCounter_);
    } else {
        AccessTime_ = ModificationTime_;
    }

    // Reconstruct TrunkNode and Transaction.
    if (TransactionId == NullTransactionId) {
        TrunkNode_ = this;
        Transaction_ = nullptr;
    } else {
        TrunkNode_ = context.Get<TCypressNodeBase>(TVersionedNodeId(Id));
        Transaction_ = context.Get<TTransaction>(TransactionId);
    }

    // Reconstruct iterators from locks to their positions in the lock list.
    for (auto it = AcquiredLocks_.begin(); it != AcquiredLocks_.end(); ++it) {
        auto* lock = *it;
        YCHECK(lock->GetState() == ELockState::Acquired);
        lock->SetLockListIterator(it);
    }
    for (auto it = PendingLocks_.begin(); it != PendingLocks_.end(); ++it) {
        auto* lock = *it;
        YCHECK(lock->GetState() == ELockState::Pending);
        lock->SetLockListIterator(it);
    }
}

TClusterResources TCypressNodeBase::GetResourceUsage() const
{
    return TClusterResources(0, 1);
}

TVersionedObjectId GetObjectId(const TCypressNodeBase* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

bool CompareObjectsForSerialization(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs)
{
    return GetObjectId(lhs) < GetObjectId(rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

