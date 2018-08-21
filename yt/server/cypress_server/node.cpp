#include "node.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/account.h>

#include <yt/server/transaction_server/transaction.h>

// COMPAT(babenko)
#include <yt/core/ytree/convert.h>
#include <yt/server/object_server/object.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id)
    : TObjectBase(id.ObjectId)
    , Acd_(this)
    , TransactionId_(id.TransactionId)
{
    if (TransactionId_) {
        Flags_.Trunk = false;
    }
}

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

TClusterResources TCypressNodeBase::GetDeltaResourceUsage() const
{
    YCHECK(!IsExternal());

    NSecurityServer::TClusterResources result;
    result.NodeCount = 1;
    return result;
}

TClusterResources TCypressNodeBase::GetTotalResourceUsage() const
{
    NSecurityServer::TClusterResources result;
    result.NodeCount = 1;
    return result;
}

bool TCypressNodeBase::IsBeingCreated() const
{
    return GetRevision() == NHydra::GetCurrentMutationContext()->GetVersion().ToRevision();
}

void TCypressNodeBase::Save(TSaveContext& context) const
{
    TObjectBase::Save(context);

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
    Save(context, Revision_);
    Save(context, Account_);
    Save(context, Acd_);
    Save(context, Opaque_);
    Save(context, AccessTime_);
    Save(context, AccessCounter_);
}

void TCypressNodeBase::Load(TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, ExternalCellTag_);
    // COMPAT(shakurov)
    if (context.GetVersion() < 700) {
        Load<bool>(context); // Drop AccountingEnabled_.
    }
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
    // COMPAT(shakurov)
    if (context.GetVersion() < 700) {
        Load<NSecurityServer::TClusterResources>(context); // Drop CachedResourceUsage_.
    }
    Load(context, Acd_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 505) {
        Load(context, Opaque_);
    } else {
        if (Attributes_) {
            auto& attributes = Attributes_->Attributes();
            static const TString opaqueAttributeName("opaque");
            auto it = attributes.find(opaqueAttributeName);
            if (it != attributes.end()) {
                const auto& value = it->second;
                try {
                    SetOpaque(NYTree::ConvertTo<bool>(value));
                } catch (...) {
                }
                attributes.erase(it);
            }
            if (Attributes_->Attributes().empty()) {
                Attributes_.reset();
            }
        }
    }
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

