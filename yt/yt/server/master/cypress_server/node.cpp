#include "node.h"
#include "shard.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;

using NTabletServer::TTabletResources;

////////////////////////////////////////////////////////////////////////////////

void TNullVersionedBuiltinAttribute::Persist(const NCellMaster::TPersistenceContext& /*context*/)
{ }

void TNullVersionedBuiltinAttribute::Persist(const NCypressServer::TCopyPersistenceContext& /*context*/)
{ }

void TTombstonedVersionedBuiltinAttribute::Persist(const NCellMaster::TPersistenceContext& /*context*/)
{ }

void TTombstonedVersionedBuiltinAttribute::Persist(const NCypressServer::TCopyPersistenceContext& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressNode::TCypressNode(TVersionedNodeId id)
    : TObject(id.ObjectId)
    , Acd_(this)
    , TransactionId_(id.TransactionId)
{
    if (TransactionId_) {
        Flags_.Trunk = false;
    }
}

TCypressNode::~TCypressNode() = default;

TInstant TCypressNode::GetTouchTime(bool branchIsOk) const
{
    YT_VERIFY(branchIsOk || IsTrunk());
    return TouchTime_;
}

void TCypressNode::SetTouchTime(TInstant touchTime, bool branchIsOk)
{
    YT_VERIFY(branchIsOk || IsTrunk());
    TouchTime_ = touchTime;
}

void TCypressNode::SetModified(EModificationType modificationType)
{
    TObject::SetModified(modificationType);

    auto* hydraContext = GetCurrentHydraContext();
    YT_VERIFY(hydraContext);

    SetModificationTime(hydraContext->GetTimestamp());
}

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

    return TClusterResources()
        .SetNodeCount(1);
}

TClusterResources TCypressNode::GetTotalResourceUsage() const
{
    return TClusterResources()
        .SetNodeCount(1);
}

TDetailedMasterMemory TCypressNode::GetDetailedMasterMemoryUsage() const
{
    TDetailedMasterMemory result;
    if (Attributes_) {
        result[EMasterMemoryType::Attributes] += Attributes_->GetMasterMemoryUsage();
    }
    return result;
}

TTabletResources TCypressNode::GetTabletResourceUsage() const
{
    return {};
}

NHydra::TRevision TCypressNode::GetNativeContentRevision() const
{
    YT_VERIFY(IsForeign());
    return NativeContentRevision_;
}

TCypressNode* TCypressNode::GetEffectiveExpirationTimeNode()
{
    TCypressNode* effectiveNode = nullptr;
    for (auto* node = this; node; node = node->GetParent()) {
        if (auto optionalExpirationTime = node->TryGetExpirationTime()) {
            if (!effectiveNode ||
                optionalExpirationTime < effectiveNode->TryGetExpirationTime())
            {
                effectiveNode = node;
            }
        }
    }
    return effectiveNode;
}

TCypressNode* TCypressNode::GetEffectiveExpirationTimeoutNode()
{
    TCypressNode* effectiveNode = nullptr;
    for (auto* node = this; node; node = node->GetParent()) {
        if (auto optionalExpirationTimeout = node->TryGetExpirationTimeout()) {
            if (!effectiveNode ||
                optionalExpirationTimeout < effectiveNode->TryGetExpirationTimeout())
            {
                effectiveNode = node;
            }
        }
    }
    return effectiveNode;
}

void TCypressNode::SetNativeContentRevision(NHydra::TRevision revision)
{
    YT_VERIFY(IsForeign());
    NativeContentRevision_ = revision;
}

bool TCypressNode::IsBeingCreated() const
{
    return GetRevision() == NHydra::GetCurrentHydraContext()->GetVersion().ToRevision();
}

bool TCypressNode::CanCacheResolve() const
{
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

void TCypressNode::CheckInvariants(TBootstrap* bootstrap) const
{
    TObject::CheckInvariants(bootstrap);

    if (IsSequoia() && IsNative()) {
        YT_VERIFY(MutableSequoiaProperties() && ImmutableSequoiaProperties());
    } else {
        YT_VERIFY(!MutableSequoiaProperties() && !ImmutableSequoiaProperties());
    }
}

void TCypressNode::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, ExternalCellTag_);
    TUniquePtrSerializer<>::Save(context, LockingState_);
    TRawNonversionedObjectPtrSerializer::Save(context, Parent_);
    Save(context, LockMode_);
    Save(context, ExpirationTime_);
    Save(context, ExpirationTimeout_);
    Save(context, CreationTime_);
    Save(context, ModificationTime_);
    Save(context, NativeContentRevision_);
    Save(context, Account_);
    Save(context, Acd_);
    Save(context, Opaque_);
    Save(context, AccessTime_);
    Save(context, TouchTime_);
    Save(context, AccessCounter_);
    Save(context, Shard_);
    Save(context, Annotation_);

    // Save/Load functions won't work for this class because of const qualifiers on fields.
    Save(context, ImmutableSequoiaProperties_.operator bool());
    if (ImmutableSequoiaProperties_) {
        Save(context, ImmutableSequoiaProperties_->Key);
        Save(context, ImmutableSequoiaProperties_->Path);
    }
    TUniquePtrSerializer<>::Save(context, MutableSequoiaProperties_);
}

void TCypressNode::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, ExternalCellTag_);
    TUniquePtrSerializer<>::Load(context, LockingState_);
    TRawNonversionedObjectPtrSerializer::Load(context, Parent_);
    Load(context, LockMode_);
    Load(context, ExpirationTime_);
    Load(context, ExpirationTimeout_);
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    Load(context, NativeContentRevision_);
    Load(context, Account_);
    Load(context, Acd_);
    Load(context, Opaque_);
    Load(context, AccessTime_);
    Load(context, TouchTime_);
    Load(context, AccessCounter_);
    Load(context, Shard_);
    Load(context, Annotation_);

    auto loadImmutableProperties = [&] () {
        if (Load<bool>(context)) {
            ImmutableSequoiaProperties_ = std::make_unique<TImmutableSequoiaProperties>(
                Load<TString>(context),
                Load<NYPath::TYPath>(context));
        }
    };

    // NB: If object is older than SequoiaMapNode reign - it should only have SequoiaProperties if it's a scion.
    // That case is handled in an appropriate class.
    if (context.GetVersion() >= EMasterReign::TablesInSequoia) {
        loadImmutableProperties();
        TUniquePtrSerializer<>::Load(context, MutableSequoiaProperties_);
    } else if (context.GetVersion() >= EMasterReign::SequoiaMapNode) {
        loadImmutableProperties();
        if (ImmutableSequoiaProperties_ && context.GetVersion() >= EMasterReign::SequoiaPropertiesBeingCreated) {
            auto beingCreated = Load<bool>(context);
            MutableSequoiaProperties_ = std::make_unique<TMutableSequoiaProperties>();
            MutableSequoiaProperties_->BeingCreated = beingCreated;
        }

        // Only Sequoia nodes should have sequoia properties, but before TablesInSequoia reign it was not the case.
        // Let's remove them after load.
        if (!IsSequoia()) {
            YT_VERIFY(!ImmutableSequoiaProperties_ ||
                ImmutableSequoiaProperties_->Key.empty() && ImmutableSequoiaProperties_->Path.empty());
            YT_VERIFY(!MutableSequoiaProperties_ || !MutableSequoiaProperties_->BeingCreated);

            ImmutableSequoiaProperties_.reset();
            MutableSequoiaProperties_.reset();
        }
    }
}

void TCypressNode::SaveEctoplasm(TStreamSaveContext& context) const
{
    TObject::SaveEctoplasm(context);

    using NYT::Save;
    Save(context, reinterpret_cast<uintptr_t>(TrunkNode_));
    Save(context, TransactionId_);
}

void TCypressNode::LoadEctoplasm(TStreamLoadContext& context)
{
    TObject::LoadEctoplasm(context);

    using NYT::Load;
    Load(context, reinterpret_cast<uintptr_t&>(TrunkNode_));
    Load(context, TransactionId_);
}

void TCypressNode::VerifySequoia() const
{
    YT_VERIFY(IsSequoia() && ImmutableSequoiaProperties() && MutableSequoiaProperties());
}

TCypressNode::TImmutableSequoiaProperties::TImmutableSequoiaProperties(NYPath::TYPath key, TString path)
    : Key(std::move(key))
    , Path(std::move(path))
{ }

void TCypressNode::TMutableSequoiaProperties::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, BeingCreated);
}

void TCypressNode::TMutableSequoiaProperties::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, BeingCreated);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId GetObjectId(const TCypressNode* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

