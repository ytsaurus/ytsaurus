#include "node.h"

#include "node_detail.h"
#include "shard.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/tablet_server/tablet_resources.h>

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
using namespace NYTree;

using NTabletServer::TTabletResources;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

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

TCompositeCypressNode* TCypressNode::GetParent() const
{
    return Parent_;
}

void TCypressNode::SetParent(TCompositeCypressNode* parent)
{
    if (Parent_ == parent) {
        return;
    }

    // Drop old parent.
    if (Parent_) {
        EraseOrCrash(Parent_->ImmediateDescendants(), this);
    }

    // Set new parent.
    Parent_ = parent;
    if (Parent_) {
        YT_VERIFY(Parent_->IsTrunk());
        InsertOrCrash(Parent_->ImmediateDescendants(), this);
    }
}

void TCypressNode::DropParent()
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

std::optional<TCypressNode::TExpirationTimeProperties::TView> TCypressNode::GetExpirationTimePropertiesView() const
{
    return TryGetExpirationTimeProperties()
        .and_then([] (auto* expirationTimeProperties) {
            return expirationTimeProperties->AsView();
        });
}

std::optional<TInstant> TCypressNode::GetExpirationTime() const
{
    return TryGetExpirationTimeProperties()
        .and_then([] (auto* expirationTimeProperties) {
            return expirationTimeProperties->GetExpiration();
        });
}

std::optional<NSecurityServer::TUserRawPtr> TCypressNode::GetExpirationTimeUser() const
{
    return TryGetExpirationTimeProperties()
        .and_then([] (auto* expirationTimeProperties) {
            return expirationTimeProperties->GetUser();
        });
}

std::optional<TInstant> TCypressNode::GetExpirationTimeLastResetTime() const
{
    return TryGetExpirationTimeProperties()
        .and_then([] (auto* expirationTimeProperties) {
            return expirationTimeProperties->GetLastResetTime();
        });
}

std::optional<TCypressNode::TExpirationTimeoutProperties::TView> TCypressNode::GetExpirationTimeoutPropertiesView() const
{
    return TryGetExpirationTimeoutProperties()
        .and_then([] (auto* expirationTimeoutProperties) {
            return expirationTimeoutProperties->AsView();
        });
}

std::optional<TDuration> TCypressNode::GetExpirationTimeout() const
{
    return TryGetExpirationTimeoutProperties()
        .and_then([] (auto* expirationTimeoutProperties) {
            return expirationTimeoutProperties->GetExpiration();
        });
}

std::optional<NSecurityServer::TUserRawPtr> TCypressNode::GetExpirationTimeoutUser() const
{
    return TryGetExpirationTimeoutProperties()
        .and_then([] (auto* expirationTimeoutProperties) {
            return expirationTimeoutProperties->GetUser();
        });
}

std::optional<TInstant> TCypressNode::GetExpirationTimeoutLastResetTime() const
{
    return TryGetExpirationTimeoutProperties()
        .and_then([] (auto* expirationTimeoutProperties) {
            return expirationTimeoutProperties->GetLastResetTime();
        });
}

TCypressNode* TCypressNode::GetEffectiveExpirationTimeNode()
{
    TCypressNode* effectiveNode = nullptr;
    for (auto* node = this; node; node = node->GetParent()) {
        if (auto optionalExpirationTime = node->GetExpirationTime()) {
            auto nodeTime = *optionalExpirationTime;
            if (!effectiveNode ||
                nodeTime < (*effectiveNode->GetExpirationTime()))
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
        if (auto optionalExpirationTimeout = node->GetExpirationTimeout()) {
            auto nodeTimeout = *optionalExpirationTimeout;
            if (!effectiveNode ||
                nodeTimeout < (*effectiveNode->GetExpirationTimeout()))
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

bool TCypressNode::CanCacheResolve() const
{
    if (!TrunkNode_->LockingState().TransactionToExclusiveLocks.empty()) {
        return false;
    }

    return
        GetNodeType() == ENodeType::Map ||
        GetType() == EObjectType::Link ||
        GetType() == EObjectType::PortalEntrance ||
        GetType() == EObjectType::Rootstock;
}

void TCypressNode::CheckInvariants(TBootstrap* bootstrap) const
{
    TObject::CheckInvariants(bootstrap);

    if (!IsTrunk() &&
        IsForeign() &&
        Transaction_ &&
        IsCypressTransactionType(Transaction_->GetType()))
    {
        YT_LOG_ALERT("External node is branched by a non-externalized Cypress Transaction (NodeId: %v)",
            GetVersionedId());
    }

    // NB: Half-constructed nodes may be abandonded as zombies and violate these invariants.
    if (IsObjectAlive(this)) {
        if (IsSequoia() && IsNative()) {
            if (!MutableSequoiaProperties()) {
                YT_LOG_ALERT("Sequoia node lacks mutable Sequoia properties (NodeId: %v)",
                    GetVersionedId());
            }

            if (MutableSequoiaProperties() &&
               !MutableSequoiaProperties()->BeingCreated &&
               !ImmutableSequoiaProperties())
            {
                YT_LOG_ALERT("Sequoia node is not being created and lacks immmutable Sequoia properties (NodeId: %v)",
                    GetVersionedId());
            }
        } else {
            if (MutableSequoiaProperties()) {
                YT_LOG_ALERT("Non-sequoia node has mutable Sequoia properties (NodeId: %v)",
                    GetVersionedId());
            }

            // TODO(aleksandra-zh): links should not be a special case here
            if (ImmutableSequoiaProperties() && GetType() != EObjectType::Link) {
                YT_LOG_ALERT("Non-sequoia node has immutable Sequoia properties (NodeId: %v)",
                    GetVersionedId());
            }
        }
    }
}

void TCypressNode::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, ExternalCellTag_);
    SaveWith<TUniquePtrSerializer<>>(context, LockingState_);
    SaveWith<TRawNonversionedObjectPtrSerializer>(context, Parent_);
    Save(context, LockMode_);
    Save(context, CreationTime_);
    Save(context, ModificationTime_);
    Save(context, NativeContentRevision_);
    Save(context, Account_);
    Save(context, Acd_);
    Save(context, Opaque_);
    Save(context, Reachable_);
    Save(context, AccessTime_);
    Save(context, TouchTime_);
    Save(context, AccessCounter_);
    Save(context, Shard_);
    Save(context, Annotation_);
    Save(context, ExpirationTimeProperties_);
    Save(context, ExpirationTimeoutProperties_);

    // Save/Load functions won't work for this class because of const qualifiers on fields.
    Save(context, ImmutableSequoiaProperties_.operator bool());
    if (ImmutableSequoiaProperties_) {
        Save(context, ImmutableSequoiaProperties_->Key);
        Save(context, ImmutableSequoiaProperties_->Path);
        Save(context, ImmutableSequoiaProperties_->ParentId);
    }
    TUniquePtrSerializer<>::Save(context, MutableSequoiaProperties_);
}

void TCypressNode::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, ExternalCellTag_);
    LoadWith<TUniquePtrSerializer<>>(context, LockingState_);
    LoadWith<TRawNonversionedObjectPtrSerializer>(context, Parent_);
    Load(context, LockMode_);

    // COMPAT(koloshmet)
    if (context.GetVersion() < EMasterReign::AuthorizedExpiration) {
        auto expirationTime = Load<TVersionedBuiltinAttribute<TInstant>>(context);
        auto expirationTimeout = Load<TVersionedBuiltinAttribute<TDuration>>(context);

        if (!expirationTime.IsNull()) {
            if (expirationTime.IsSet()) {
                ExpirationTimeProperties_.Set(TClonableBuiltinAttributePtr<TExpirationTimeProperties>(
                    std::in_place, TUserPtr{}, expirationTime.Unbox()));
            } else {
                ExpirationTimeProperties_.Remove();
            }
        }
        if (!expirationTimeout.IsNull()) {
            if (expirationTimeout.IsSet()) {
                ExpirationTimeoutProperties_.Set(TClonableBuiltinAttributePtr<TExpirationTimeoutProperties>(
                    std::in_place, TUserPtr{}, expirationTimeout.Unbox()));
            } else {
                ExpirationTimeoutProperties_.Remove();
            }
        }
    }
    Load(context, CreationTime_);
    Load(context, ModificationTime_);
    Load(context, NativeContentRevision_);
    Load(context, Account_);
    Load(context, Acd_);
    Load(context, Opaque_);
    Load(context, Reachable_);
    Load(context, AccessTime_);
    Load(context, TouchTime_);
    Load(context, AccessCounter_);
    Load(context, Shard_);
    Load(context, Annotation_);

    // COMPAT(koloshmet)
    if (context.GetVersion() >= EMasterReign::AuthorizedExpiration) {
        Load(context, ExpirationTimeProperties_);
        Load(context, ExpirationTimeoutProperties_);
    }

    if (Load<bool>(context)) {
        auto key = Load<std::string>(context);
        auto path = Load<TYPath>(context);
        auto parentId = Load<TNodeId>(context);
        ImmutableSequoiaProperties_ = std::make_unique<TImmutableSequoiaProperties>(
            std::move(key),
            std::move(path),
            parentId);
    }
    TUniquePtrSerializer<>::Load(context, MutableSequoiaProperties_);
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
    YT_VERIFY(IsSequoia() && MutableSequoiaProperties());
    YT_VERIFY(MutableSequoiaProperties()->BeingCreated || ImmutableSequoiaProperties());
}

TCypressNode::TImmutableSequoiaProperties::TImmutableSequoiaProperties(
    std::string key,
    TYPath path,
    TNodeId parentId)
    : Key(std::move(key))
    , Path(std::move(path))
    , ParentId(parentId)
{ }

void TCypressNode::TMutableSequoiaProperties::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, BeingCreated);
    Save(context, Tombstone);
}

void TCypressNode::TMutableSequoiaProperties::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, BeingCreated);
    Load(context, Tombstone);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId GetObjectId(const TCypressNode* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

