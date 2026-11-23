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

#include <yt/yt/core/phoenix/type_def.h>

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

void TNullVersionedBuiltinAttribute::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TNullVersionedBuiltinAttribute);

////////////////////////////////////////////////////////////////////////////////

void TTombstonedVersionedBuiltinAttribute::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TTombstonedVersionedBuiltinAttribute);

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_TEMPLATE_TYPE(TVersionedBuiltinAttribute, (bool));
PHOENIX_DEFINE_TEMPLATE_TYPE(TExpirationValue, (TInstant));
PHOENIX_DEFINE_TEMPLATE_TYPE(TCypressNode::TExpirationProperties, (TInstant));

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

TInstant TCypressNode::GetTouchTime() const
{
    YT_TLOG_ALERT_AND_THROW_UNLESS(IsTrunk(),
        "Attempted to access the touch time of a branch")
        .With("NodeId", GetVersionedId());

    return TouchTime_;
}

void TCypressNode::SetTouchTime(TInstant touchTime)
{
    YT_VERIFY(IsTrunk());
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

std::optional<TInstant> TCypressNode::GetExpirationTimeArmingTime() const
{
    return TryGetExpirationTimeProperties()
        .and_then([] (auto* expirationTimeProperties) {
            return expirationTimeProperties->GetArmingTime();
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

std::optional<TInstant> TCypressNode::GetExpirationTimeoutArmingTime() const
{
    return TryGetExpirationTimeoutProperties()
        .and_then([] (auto* expirationTimeoutProperties) {
            return expirationTimeoutProperties->GetArmingTime();
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

    YT_TLOG_DEBUG("Setting native content revision")
        .With("NodeId", GetVersionedId())
        .With("OldRevision", NativeContentRevision_)
        .With("NewRevision", revision);

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
        YT_TLOG_ALERT("External node is branched by a non-externalized Cypress Transaction")
            .With("NodeId", GetVersionedId());
    }

    // NB: Half-constructed nodes may be abandonded as zombies and violate these invariants.
    if (IsObjectAlive(this)) {
        if (IsSequoia() && IsNative()) {
            if (!MutableSequoiaProperties()) {
                YT_TLOG_ALERT("Sequoia node lacks mutable Sequoia properties")
                    .With("NodeId", GetVersionedId());
            }

            if (MutableSequoiaProperties() &&
               !MutableSequoiaProperties()->BeingCreated &&
               !ImmutableSequoiaProperties())
            {
                YT_TLOG_ALERT("Sequoia node is not being created and lacks immmutable Sequoia properties")
                    .With("NodeId", GetVersionedId());
            }
        } else {
            if (MutableSequoiaProperties()) {
                YT_TLOG_ALERT("Non-sequoia node has mutable Sequoia properties")
                    .With("NodeId", GetVersionedId());
            }

            // TODO(aleksandra-zh): links should not be a special case here
            if (ImmutableSequoiaProperties() && GetType() != EObjectType::Link) {
                YT_TLOG_ALERT("Non-sequoia node has immutable Sequoia properties")
                    .With("NodeId", GetVersionedId());
            }
        }
    }
}

void TCypressNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, ExternalCellTag_);
    PHOENIX_REGISTER_FIELD(2, LockingState_,
        .template Serializer<TUniquePtrSerializer<>>());
    PHOENIX_REGISTER_FIELD(3, Parent_,
        .template Serializer<TRawNonversionedObjectPtrSerializer>());
    PHOENIX_REGISTER_FIELD(4, LockMode_);

    // COMPAT(koloshmet)
    registrar.template VirtualField<5>("LegacyExpiration", [] (TThis* this_, auto& context) {
        using NYT::Load;
        auto expirationTime = Load<TVersionedBuiltinAttribute<TInstant>>(context);
        auto expirationTimeout = Load<TVersionedBuiltinAttribute<TDuration>>(context);

        if (!expirationTime.IsNull()) {
            if (expirationTime.IsSet()) {
                this_->ExpirationTimeProperties_.Set(TClonableBuiltinAttributePtr<TExpirationTimeProperties>(
                    std::in_place, TUserPtr{}, expirationTime.Unbox(), TInstant::Zero()));
            } else {
                this_->ExpirationTimeProperties_.Remove();
            }
        }
        if (!expirationTimeout.IsNull()) {
            if (expirationTimeout.IsSet()) {
                this_->ExpirationTimeoutProperties_.Set(TClonableBuiltinAttributePtr<TExpirationTimeoutProperties>(
                    std::in_place, TUserPtr{}, expirationTimeout.Unbox(), TInstant::Zero()));
            } else {
                this_->ExpirationTimeoutProperties_.Remove();
            }
        }
    })
        .BeforeVersion(EMasterReign::AuthorizedExpiration)();

    PHOENIX_REGISTER_FIELD(6, CreationTime_);
    PHOENIX_REGISTER_FIELD(7, ModificationTime_);
    PHOENIX_REGISTER_FIELD(8, NativeContentRevision_);
    PHOENIX_REGISTER_FIELD(9, Account_);
    PHOENIX_REGISTER_FIELD(10, Acd_);
    PHOENIX_REGISTER_FIELD(11, Opaque_);
    PHOENIX_REGISTER_FIELD(12, Reachable_);
    PHOENIX_REGISTER_FIELD(13, AccessTime_);
    PHOENIX_REGISTER_FIELD(14, TouchTime_);
    PHOENIX_REGISTER_FIELD(15, AccessCounter_);
    PHOENIX_REGISTER_FIELD(16, Shard_);
    PHOENIX_REGISTER_FIELD(17, Annotation_);

    // COMPAT(koloshmet): when missing, keep the values possibly set by LegacyExpiration.
    PHOENIX_REGISTER_FIELD(18, ExpirationTimeProperties_,
        .SinceVersion(EMasterReign::AuthorizedExpiration)
        .KeepWhenMissing());
    PHOENIX_REGISTER_FIELD(19, ExpirationTimeoutProperties_,
        .SinceVersion(EMasterReign::AuthorizedExpiration)
        .KeepWhenMissing());

    // NB: TImmutableSequoiaProperties has const fields, hence no Save/Load of its own.
    registrar.template VirtualField<20>("ImmutableSequoiaProperties_", [] (TThis* this_, auto& context) {
        using NYT::Load;
        if (Load<bool>(context)) {
            auto key = Load<std::string>(context);
            auto path = Load<TYPath>(context);
            auto parentId = Load<TNodeId>(context);
            this_->ImmutableSequoiaProperties_ = std::make_unique<TImmutableSequoiaProperties>(
                std::move(key),
                std::move(path),
                parentId);
        }
    }, [] (const TThis* this_, auto& context) {
        using NYT::Save;
        const auto& properties = this_->ImmutableSequoiaProperties_;
        Save(context, properties.operator bool());
        if (properties) {
            Save(context, properties->Key);
            Save(context, properties->Path);
            Save(context, properties->ParentId);
        }
    })();

    PHOENIX_REGISTER_FIELD(21, MutableSequoiaProperties_,
        .template Serializer<TUniquePtrSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TCypressNode);

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

void TCypressNode::TMutableSequoiaProperties::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, BeingCreated);
    PHOENIX_REGISTER_FIELD(2, Tombstone);
}

PHOENIX_DEFINE_TYPE(TCypressNode::TMutableSequoiaProperties);

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId GetObjectId(const TCypressNode* object)
{
    return object ? object->GetVersionedId() : TVersionedObjectId(NullObjectId, NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
