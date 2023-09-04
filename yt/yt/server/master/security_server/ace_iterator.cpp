#include "ace_iterator.h"

#include <yt/yt/server/master/object_server/type_handler.h>
#include <yt/yt/server/master/object_server/object_manager.h>


namespace NYT::NSecurityServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TAceIterator::TAceIterator(
    const IObjectManagerPtr& objectManager,
    TObject* object,
    TAcdOverride firstObjectAcdOverride)
    : ObjectManager_(objectManager.Get())
    , FirstObjectAcdOverride_(std::move(firstObjectAcdOverride))
{
    if (object == nullptr) {
        return;
    }

    BindToObject(object);

    Owner_ = FirstObjectAcdOverride_.Owner.value_or(Acd_ ? Acd_->GetOwner() : nullptr);

    if (FirstObjectAcdOverride_.Acl) {
        if (FirstObjectAcdOverride_.Acl->empty()) {
            GoToNextObject();
            GoToFirstNonEmptyAncestor();
        } else {
            const auto& acl = FirstObjectAcdOverride_.Acl.value();
            Ace_ = acl.begin();
            AclEnd_ = acl.end();
        }
    } else {
        GoToFirstNonEmptyAncestor();
    }
}

TAceIterator& TAceIterator::operator++()
{
    ++Ace_;
    if (Ace_ != AclEnd_) {
        return *this;
    }
    GoToNextObject();
    GoToFirstNonEmptyAncestor();
    return *this;
}

TAceIterator TAceIterator::End()
{
    return {};
}

// Binds to the parent object if there is one and if inheritance is enabled,
// otherwise finalizes iteration.
void TAceIterator::GoToNextObject()
{
    YT_VERIFY(Handler_);
    if (!AcquireInheritanceModifier()) {
        FirstObjectAcdOverride_.InheritAcl.reset();
        FinalizeAcdIteration(EAceIteratorStopCause::InheritanceDisabled);
        return;
    }
    FirstObjectAcdOverride_.InheritAcl.reset();
    auto *parent = Handler_->GetParent(Object_);
    if (parent) {
        ++Depth_;
        BindToObject(parent);
    } else {
        FinalizeAcdIteration(EAceIteratorStopCause::HasNoParent);
    }
}

void TAceIterator::BindToObject(TObject* object)
{
    Object_= object;
    Handler_ = ObjectManager_->GetHandler(Object_);
    Acd_ = Handler_->FindAcd(Object_);
}

// Binds to the first found ACE if there is, finalizes otherwise.
void TAceIterator::GoToFirstNonEmptyAncestor()
{
    while (!IsIterationFinished() &&
        (!Acd_ || Acd_->Acl().Entries.empty()))
    {
        GoToNextObject();
    }

    if (IsIterationFinished()) {
        return;
    }

    const auto& entries = Acd_->Acl().Entries;
    Ace_ = entries.begin();
    AclEnd_ = entries.end();
}

void TAceIterator::FinalizeAcdIteration(EAceIteratorStopCause cause) noexcept
{
    StopCause_ = cause;

    Handler_ = nullptr;
    Acd_ = nullptr;
    Ace_ = nullptr;
    AclEnd_ = nullptr;
}

bool TAceIterator::AcquireInheritanceModifier() noexcept
{
    auto ans = FirstObjectAcdOverride_
        .InheritAcl
        .value_or(Acd_ == nullptr || Acd_->GetInherit());
    FirstObjectAcdOverride_.InheritAcl.reset();
    return ans;
}

Y_FORCE_INLINE bool TAceIterator::IsIterationFinished() noexcept
{
    return StopCause_ != EAceIteratorStopCause::Undefined;
}

} // namespace NYT::NSecurityServer
