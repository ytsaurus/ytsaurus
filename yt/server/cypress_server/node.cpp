#include "stdafx.h"
#include "node.h"

#include <server/security_server/account.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;

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
    return TVersionedNodeId(Id_, TransactionId);
}

void TCypressNodeBase::Save(TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, LockStateMap_);
    Save(context, AcquiredLocks_);
    Save(context, PendingLocks_);
    TNonversionedObjectRefSerializer::Save(context, Parent_);
    Save(context, LockMode_);
    Save(context, CreationTime_);
    Save(context, ModificationTime_);
    Save(context, Revision_);
    Save(context, Account_);
    Save(context, CachedResourceUsage_);
    Save(context, Acd_);
    Save(context, AccessTime_);
    Save(context, AccessCounter_);
}

void TCypressNodeBase::Load(TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, LockStateMap_);
    Load(context, AcquiredLocks_);
    Load(context, PendingLocks_);
    TNonversionedObjectRefSerializer::Load(context, Parent_);
    Load(context, LockMode_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    Load(context, Revision_);
    Load(context, Account_);
    Load(context, CachedResourceUsage_);
    Load(context, Acd_);
    Load(context, AccessTime_);
    Load(context, AccessCounter_);

    // Reconstruct TrunkNode and Transaction.
    if (TransactionId == NullTransactionId) {
        TrunkNode_ = this;
        Transaction_ = nullptr;
    } else {
        TrunkNode_ = context.Get<TCypressNodeBase>(TVersionedNodeId(Id_));
        Transaction_ = context.Get<TTransaction>(TransactionId);
    }

    // Reconstruct iterators from locks to their positions in the lock list.
    for (auto it = AcquiredLocks_.begin(); it != AcquiredLocks_.end(); ++it) {
        auto* lock = *it;
        lock->SetLockListIterator(it);
    }
    for (auto it = PendingLocks_.begin(); it != PendingLocks_.end(); ++it) {
        auto* lock = *it;
        lock->SetLockListIterator(it);
    }
}

TClusterResources TCypressNodeBase::GetResourceUsage() const
{
    return TClusterResources(0, 1, 0);
}

TVersionedObjectId GetObjectId(const TCypressNodeBase* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

