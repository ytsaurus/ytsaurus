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

    if (Parent_) {
        YCHECK(Parent_->ImmediateAncestors().erase(this) == 1);
    }
    Parent_ = newParent;
    if (Parent_) {
        YCHECK(Parent_->ImmediateAncestors().insert(this).second);
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

int TCypressNodeBase::GetOwningReplicationFactor() const 
{
    YUNREACHABLE();
}

void TCypressNodeBase::Save(const NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRefs(output, Locks_);

    // TODO(babenko): refactor when new serialization API is ready
    auto parentId = Parent_ ? Parent_->GetId() : NullObjectId;
    NYT::Save(output, parentId);

    ::Save(output, LockMode_);
    ::Save(output, CreationTime_);
    ::Save(output, ModificationTime_);
    SaveObjectRef(output, Account_);
    NSecurityServer::Save(output, CachedResourceUsage_);
}

void TCypressNodeBase::Load(const TLoadContext& context)
{
    TObjectBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRefs(input, Locks_, context);
    
    // TODO(babenko): refactor when new serialization API is ready
    TNodeId parentId;
    NYT::Load(input, parentId);
    Parent_ = parentId == NullObjectId ? nullptr : context.Get<TCypressNodeBase>(parentId);
    
    ::Load(input, LockMode_);
    ::Load(input, CreationTime_);
    ::Load(input, ModificationTime_);
    LoadObjectRef(input, Account_, context);
    NSecurityServer::Load(input, CachedResourceUsage_);

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
    return ZeroClusterResources();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

