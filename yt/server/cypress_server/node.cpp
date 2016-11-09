#include "node.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/account.h>

#include <yt/server/transaction_server/transaction.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id)
    : TObjectBase(id.ObjectId)
    , ExternalCellTag_(NotReplicatedCellTag)
    , AccountingEnabled_(true)
    , LockMode_(ELockMode::None)
    , TrunkNode_(nullptr)
    , Transaction_(nullptr)
    , CreationTime_(0)
    , ModificationTime_(0)
    , AccessTime_(0)
    , AccessCounter_(0)
    , Revision_(0)
    , Account_(nullptr)
    , Acd_(this)
    , Parent_(nullptr)
    , Originator_(nullptr)
    , TransactionId_(id.TransactionId)
{ }

TCypressNodeBase::~TCypressNodeBase() = default;

TCypressNodeBase* TCypressNodeBase::GetParent() const
{
    return Parent_;
}

void TCypressNodeBase::SetParent(TCypressNodeBase* parent)
{
    if (Parent_ == parent)
        return;

    // Drop old parent.
    if (Parent_) {
        YCHECK(Parent_->ImmediateDescendants().erase(this) == 1);
    }

    // Set new parent.
    Parent_ = parent;
    if (Parent_) {
        YCHECK(Parent_->IsTrunk());
        YCHECK(Parent_->ImmediateDescendants().insert(this).second);
    }
}

void TCypressNodeBase::ResetParent()
{
    Parent_ = nullptr;
}

TCypressNodeBase* TCypressNodeBase::GetOriginator() const
{
    return Originator_;
}

void TCypressNodeBase::SetOriginator(TCypressNodeBase* originator)
{
    Originator_ = originator;
}

const TCypressNodeLockingState& TCypressNodeBase::LockingState() const
{
    return LockingState_ ? *LockingState_ : TCypressNodeLockingState::Empty;
}

TCypressNodeLockingState* TCypressNodeBase::MutableLockingState()
{
    if (!LockingState_) {
        LockingState_ = std::make_unique<TCypressNodeLockingState>();
    }
    return LockingState_.get();
}

bool TCypressNodeBase::HasLockingState() const
{
    return LockingState_.operator bool();
}

void TCypressNodeBase::ResetLockingState()
{
    LockingState_.reset();
}

void TCypressNodeBase::ResetLockingStateIfEmpty()
{
    if (LockingState_ && LockingState_->IsEmpty()) {
        LockingState_.reset();
    }
}

TVersionedNodeId TCypressNodeBase::GetVersionedId() const
{
    return TVersionedNodeId(Id_, TransactionId_);
}

bool TCypressNodeBase::IsExternal() const
{
    return ExternalCellTag_ >= MinValidCellTag && ExternalCellTag_ <= MaxValidCellTag;
}

void TCypressNodeBase::Save(TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, ExternalCellTag_);
    Save(context, AccountingEnabled_);
    if (LockingState_) {
        Save(context, true);
        Save(context, *LockingState_);
    } else {
        Save(context, false);
    }
    TNonversionedObjectRefSerializer::Save(context, Parent_);
    Save(context, LockMode_);
    Save(context, ExpirationTime_);
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
    Load(context, ExternalCellTag_);
    Load(context, AccountingEnabled_);
    // COMPAT(babenko)
    if (context.GetVersion() < 400) {
        YCHECK(TSizeSerializer::Load(context) == 0);
        YCHECK(TSizeSerializer::Load(context) == 0);
        YCHECK(TSizeSerializer::Load(context) == 0);
    } else {
        if (Load<bool>(context)) {
            LockingState_ = std::make_unique<TCypressNodeLockingState>();
            Load(context, *LockingState_);
        }
    }
    TNonversionedObjectRefSerializer::Load(context, Parent_);
    Load(context, LockMode_);
    Load(context, ExpirationTime_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    Load(context, Revision_);
    Load(context, Account_);
    Load(context, CachedResourceUsage_);
    Load(context, Acd_);
    Load(context, AccessTime_);
    Load(context, AccessCounter_);
}

TVersionedObjectId GetObjectId(const TCypressNodeBase* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

