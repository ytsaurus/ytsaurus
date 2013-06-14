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
    , Account_(nullptr)
    , CachedResourceUsage_(ZeroClusterResources())
    , Acd_(this)
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
    SaveObjectRefs(context, Locks_);
    // TODO(babenko): refactor when new serialization API is ready
    auto parentId = Parent_ ? Parent_->GetId() : NullObjectId;
    Save(context, parentId);
    Save(context, LockMode_);
    Save(context, CreationTime_);
    Save(context, ModificationTime_);
    SaveObjectRef(context, Account_);
    Save(context, CachedResourceUsage_);
    Save(context, Acd_);
}

void TCypressNodeBase::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    LoadObjectRefs(context, Locks_);
    // TODO(babenko): refactor when new serialization API is ready
    TNodeId parentId;
    Load(context, parentId);
    Parent_ = parentId == NullObjectId ? nullptr : context.Get<TCypressNodeBase>(parentId);
    Load(context, LockMode_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    LoadObjectRef(context, Account_);
    Load(context, CachedResourceUsage_);
    Load(context, Acd_);

    if (TransactionId == NullTransactionId) {
        TrunkNode_ = this;
        Transaction_ = nullptr;
    } else {
        TrunkNode_ = context.Get<TCypressNodeBase>(TVersionedNodeId(Id));
        Transaction_ = context.Get<TTransaction>(TransactionId);
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

