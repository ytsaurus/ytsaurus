#include "chaos_lease.h"

#include "serialize.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

bool TChaosLease::IsRoot() const
{
    return !RootId_;
}

bool TChaosLease::IsNormalState() const
{
    return State_ == EChaosLeaseState::Normal;
}

void TChaosLease::SetState(EChaosLeaseState newState)
{
    State_ = newState;

    if (State_ == EChaosLeaseState::RevokingShortcutsForRemoval) {
        if (!RemovePromise_) {
            RemovePromise_ = NewPromise<void>();
        }
    }
}

EChaosLeaseState TChaosLease::GetState() const
{
    return State_;
}

void TChaosLease::Save(TSaveContext& context) const
{
    TChaosObjectBase::Save(context);

    using NYT::Save;

    Save(context, RootId_);
    Save(context, ParentId_);
    Save(context, NestedLeaseIds_);
    Save(context, Timeout_);
    Save(context, State_);
}

void TChaosLease::Load(TLoadContext& context)
{
    if (context.GetVersion() >= EChaosReign::FixChaosLeasePersist) {
        TChaosObjectBase::Load(context);
    }

    using NYT::Load;

    if (context.GetVersion() >= EChaosReign::IntroduceChaosLeaseManager) {
        Load(context, RootId_);
    }

    Load(context, ParentId_);
    Load(context, NestedLeaseIds_);
    Load(context, Timeout_);

    auto state = Load<EChaosLeaseState>(context);
    SetState(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
