#include "node.h"
#include "shard.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/security_server/account.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/server/master/object_server/object.h>

namespace NYT::NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TCypressNode::TCypressNode(const TVersionedNodeId& id)
    : TObject(id.ObjectId)
    , Acd_(this)
    , TransactionId_(id.TransactionId)
{
    if (TransactionId_) {
        Flags_.Trunk = false;
    }
}

TCypressNode::~TCypressNode() = default;

TCypressNode* TCypressNode::GetParent() const
{
    return Parent_;
}

void TCypressNode::SetParent(TCypressNode* parent)
{
    if (Parent_ == parent)
        return;

    // Drop old parent.
    if (Parent_) {
        YT_VERIFY(Parent_->ImmediateDescendants().erase(this) == 1);
    }

    // Set new parent.
    Parent_ = parent;
    if (Parent_) {
        YT_VERIFY(Parent_->IsTrunk());
        YT_VERIFY(Parent_->ImmediateDescendants().insert(this).second);
    }
}

void TCypressNode::ResetParent()
{
    Parent_ = nullptr;
}

TCypressNode* TCypressNode::GetOriginator() const
{
    return Originator_;
}

void TCypressNode::SetOriginator(TCypressNode* originator)
{
    Originator_ = originator;
}

const TCypressNodeLockingState& TCypressNode::LockingState() const
{
    return LockingState_ ? *LockingState_ : TCypressNodeLockingState::Empty;
}

TCypressNodeLockingState* TCypressNode::MutableLockingState()
{
    if (!LockingState_) {
        LockingState_ = std::make_unique<TCypressNodeLockingState>();
    }
    return LockingState_.get();
}

bool TCypressNode::HasLockingState() const
{
    return LockingState_.operator bool();
}

void TCypressNode::ResetLockingState()
{
    LockingState_.reset();
}

void TCypressNode::ResetLockingStateIfEmpty()
{
    if (LockingState_ && LockingState_->IsEmpty()) {
        LockingState_.reset();
    }
}

TVersionedNodeId TCypressNode::GetVersionedId() const
{
    return TVersionedNodeId(Id_, TransactionId_);
}

bool TCypressNode::IsExternal() const
{
    return ExternalCellTag_ >= MinValidCellTag && ExternalCellTag_ <= MaxValidCellTag;
}

TClusterResources TCypressNode::GetDeltaResourceUsage() const
{
    YT_VERIFY(!IsExternal());

    NSecurityServer::TClusterResources result;
    result.NodeCount = 1;
    return result;
}

TClusterResources TCypressNode::GetTotalResourceUsage() const
{
    NSecurityServer::TClusterResources result;
    result.NodeCount = 1;
    return result;
}

i64 TCypressNode::GetMasterMemoryUsage() const
{
    i64 result = 0;
    if (Attributes_) {
        result += Attributes_->GetMasterMemoryUsage();
    }
    return result;
}

NHydra::TRevision TCypressNode::GetRevision() const
{
    return Max(AttributesRevision_, ContentRevision_);
}

bool TCypressNode::IsBeingCreated() const
{
    return GetRevision() == NHydra::GetCurrentMutationContext()->GetVersion().ToRevision();
}

bool TCypressNode::CanCacheResolve() const
{
    if (!TrunkNode_->LockingState().KeyToSharedLocks.empty()) {
        return false;
    }
    if (!TrunkNode_->LockingState().TransactionToExclusiveLocks.empty()) {
        return false;
    }
    if (GetNodeType() != NYTree::ENodeType::Map &&
        GetType() != EObjectType::Link &&
        GetType() != EObjectType::PortalEntrance)
    {
        return false;
    }
    return true;
}

void TCypressNode::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, ExternalCellTag_);
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
    Save(context, AttributesRevision_);
    Save(context, ContentRevision_);
    Save(context, Account_);
    Save(context, Acd_);
    Save(context, Opaque_);
    Save(context, AccessTime_);
    Save(context, AccessCounter_);
    Save(context, Shard_);
    Save(context, Annotation_);
}

void TCypressNode::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, ExternalCellTag_);
    if (Load<bool>(context)) {
        LockingState_ = std::make_unique<TCypressNodeLockingState>();
        Load(context, *LockingState_);
    }
    TNonversionedObjectRefSerializer::Load(context, Parent_);
    Load(context, LockMode_);
    Load(context, ExpirationTime_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    Load(context, AttributesRevision_);
    Load(context, ContentRevision_);
    Load(context, Account_);
    Load(context, Acd_);
    Load(context, Opaque_);
    Load(context, AccessTime_);
    Load(context, AccessCounter_);
    // COMPAT(babenko)
    if (context.GetVersion() >= EMasterReign::CypressShards) {
        Load(context, Shard_);
    }
    // COMPAT(avmatrosov)
    if (context.GetVersion() >= EMasterReign::YT_10745_Annotation) {
        Load(context, Annotation_);
    }
}

TVersionedObjectId GetObjectId(const TCypressNode* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

