#include "ace_iterator.h"

#include <yt/yt/server/master/object_server/type_handler.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/user.h>

namespace NYT::NSecurityServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TAclInheritanceIterator::TAclInheritanceIterator(
    const IObjectManager* objectManager,
    TObject* begin)
    : ObjectManager_(objectManager)
    , Object_(begin)
{
    YT_VERIFY(ObjectManager_);

    OnObjectUpdated();
}

TAclInheritanceIterator& TAclInheritanceIterator::operator++()
{
    return DoIncrement();
}

TAclInheritanceIterator& TAclInheritanceIterator::IncrementIngoreInheritAcl()
{
    return DoIncrement(true);
}

TAclInheritanceIterator& TAclInheritanceIterator::DoIncrement(bool ignoreInheritAcl)
{
    YT_VERIFY(ObjectTypeHandler_);
    YT_VERIFY(Object_);

    auto* acd = ObjectTypeHandler_->FindAcd(Object_);
    if (ignoreInheritAcl || !acd || acd->Inherit()) {
        Object_ = ObjectTypeHandler_->GetParent(Object_);
        OnObjectUpdated();
    } else {
        MarkAsEnd();
        // NB: Object_ is left non-null, cf. GetStopCause.
    }

    return *this;
}

bool TAclInheritanceIterator::operator==(const TAclInheritanceIterator& rhs) const
{
    // End iterators.
    if (!ObjectManager_ || !rhs.ObjectManager_) {
        return ObjectManager_ == rhs.ObjectManager_;
    }

    return Object_ == rhs.Object_;
}

TAclInheritanceIterator::TValue TAclInheritanceIterator::operator*() const
{
    YT_VERIFY(ObjectTypeHandler_);
    YT_VERIFY(Object_);

    auto* acd = ObjectTypeHandler_->FindAcd(Object_);

    return {
        .Object = Object_,
        .Acl = acd ? &acd->Acl() : nullptr,
        .InheritAcl = acd ? &acd->Inherit() : nullptr,
    };
}

EAceIteratorStopCause TAclInheritanceIterator::GetStopCause() const
{
    if (ObjectManager_) {
        return EAceIteratorStopCause::None;
    }

    return Object_
        ? EAceIteratorStopCause::InheritanceDisabled
        : EAceIteratorStopCause::NoParent;
}

void TAclInheritanceIterator::OnObjectUpdated()
{
    if (Object_) {
        UpdateTypeHandler();
    } else {
        MarkAsEnd();
    }
}

void TAclInheritanceIterator::MarkAsEnd()
{
    ObjectManager_ = nullptr;
    ObjectTypeHandler_ = nullptr;
}

void TAclInheritanceIterator::UpdateTypeHandler()
{
    ObjectTypeHandler_ = ObjectManager_->GetHandler(Object_).Get();
    YT_VERIFY(ObjectTypeHandler_);
}

////////////////////////////////////////////////////////////////////////////////

TAcdOverride::TAcdOverride(TAccessControlList acl)
    : Acl_(std::move(acl))
{ }

TAcdOverride::TAcdOverride(bool inheritAcl)
    : InheritAcl_(inheritAcl)
{ }

TAcdOverride::TAcdOverride(TSubject& owner)
    : Owner_(&owner)
{ }

void TAcdOverride::Reset()
{
    Acl_.reset();
    InheritAcl_.reset();
    Owner_.reset();
}

TAcdOverride::operator bool() const
{
    return Acl_ || InheritAcl_ || Owner_;
}

////////////////////////////////////////////////////////////////////////////////

TOverriddenBeginAclInheritanceIterator::TOverriddenBeginAclInheritanceIterator(
    const IObjectManager* objectManager,
    TObject* begin,
    TAcdOverride beginOverride)
    : BeginOverride_(std::move(beginOverride))
    , Underlying_(objectManager, begin)
{
    YT_VERIFY(HasOverride());

    if (GetStopCause() != EAceIteratorStopCause::None) {
        ResetOverride();
    }
}

TOverriddenBeginAclInheritanceIterator& TOverriddenBeginAclInheritanceIterator::operator++()
{
    if (BeginOverride_.InheritAcl()) {
        auto underlyingValue = *Underlying_;
        auto originalInheritAcl = underlyingValue.InheritAcl
            ? *underlyingValue.InheritAcl
            : true;
        auto overriddenInheritAcl = *BeginOverride_.InheritAcl();

        if (originalInheritAcl == overriddenInheritAcl) {
            ++Underlying_;
        } else if (overriddenInheritAcl) {
            // Forcing inheritance by manually skipping to the parent object.
            Underlying_.IncrementIngoreInheritAcl();
        } else {
            // Forcibly suppressing inheritance by manually skipping to end.
            Underlying_ = TAclInheritanceIterator();
        }
    } else {
        ++Underlying_;
    }

    ResetOverride();

    return *this;
}

bool TOverriddenBeginAclInheritanceIterator::operator==(const TThis& rhs) const
{
    if (HasOverride() || rhs.HasOverride()) {
        return HasOverride() == rhs.HasOverride() &&
            BeginOverride_ == rhs.BeginOverride_;
    }

    return Underlying_ == rhs.Underlying_;
}

bool TOverriddenBeginAclInheritanceIterator::operator==(
    const TAclInheritanceIterator& rhs) const
{
    if (HasOverride()) {
        return false;
    }

    return Underlying_ == rhs;
}

TOverriddenBeginAclInheritanceIterator::TValue
TOverriddenBeginAclInheritanceIterator::operator*() const
{
    return HasOverride()
        ? GetOveriddenValue()
        : *Underlying_;
}
EAceIteratorStopCause TOverriddenBeginAclInheritanceIterator::GetStopCause() const
{
    return Underlying_.GetStopCause();
}

bool TOverriddenBeginAclInheritanceIterator::HasOverride() const
{
    return BeginOverride_.operator bool();
}

void TOverriddenBeginAclInheritanceIterator::ResetOverride()
{
    BeginOverride_.Reset();
}

TOverriddenBeginAclInheritanceIterator::TValue
TOverriddenBeginAclInheritanceIterator::GetOveriddenValue() const
{
    auto result = *Underlying_;
    if (BeginOverride_.Acl()) {
        result.Acl = &*BeginOverride_.Acl();
    }
    if (BeginOverride_.InheritAcl()) {
        result.InheritAcl = &*BeginOverride_.InheritAcl();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TAceIterator::TAceIterator(
    const IObjectManager* objectManager,
    TObject* object,
    TAcdOverride firstObjectAcdOverride)
{
    if (firstObjectAcdOverride) {
        Underlying_ = TOverriddenBeginAclInheritanceIterator(
            objectManager,
            object,
            std::move(firstObjectAcdOverride));
    } else {
        Underlying_ = TAclInheritanceIterator(objectManager, object);
    }

    SkipNullUnderlying();
    UpdateAclIterator();
}

void TAceIterator::UpdateAclIterator()
{
    if (GetStopCause() == EAceIteratorStopCause::None) {
        auto* acl = DereferenceUnderlying().Acl;
        YT_VERIFY(acl);
        AclIterator_ = acl->Entries.cbegin();
    }
}

EAceIteratorStopCause TAceIterator::GetStopCause() const
{
    return Visit(
        Underlying_,
        [&] (const TAclInheritanceIterator& underlying) {
            return underlying.GetStopCause();
        },
        [&] (const TOverriddenBeginAclInheritanceIterator& underlying) {
            return underlying.GetStopCause();
        });
}

TAclInheritanceIterator::TValue TAceIterator::DereferenceUnderlying() const
{
    return Visit(
        Underlying_,
        [&] (const TAclInheritanceIterator& underlying) {
            return *underlying;
        },
        [&] (const TOverriddenBeginAclInheritanceIterator& underlying) {
            return *underlying;
        });
}

void TAceIterator::SkipNullUnderlying()
{
    DoAdvanceUnderlying(true);
}

void TAceIterator::AdvanceUnderlying()
{
    DoAdvanceUnderlying(false);
}

void TAceIterator::DoAdvanceUnderlying(bool checkFirst)
{
    auto shouldStop = [&] {
        if (GetStopCause() != EAceIteratorStopCause::None) {
            return true;
        }
        auto value = DereferenceUnderlying();
        return value.Acl && ! value.Acl->Entries.empty();
    };

    if (checkFirst && shouldStop()) {
        return;
    }

    do {
        Visit(
            Underlying_,
            [&] (TAclInheritanceIterator& underlying) {
                ++underlying;
            },
            [&] (TOverriddenBeginAclInheritanceIterator& underlying) {
                ++underlying;
            });

        ++ObjectsTraversed_;

    } while (!shouldStop());

    UpdateAclIterator();
}

TAceIterator& TAceIterator::operator++()
{
    auto value = DereferenceUnderlying();
    YT_VERIFY(value.Acl);
    if (++AclIterator_ == value.Acl->Entries.end()) {
        AdvanceUnderlying();
    }

    return *this;
}

bool TAceIterator::operator==(const TAceIterator& rhs) const
{
    if (GetStopCause() != EAceIteratorStopCause::None ||
        rhs.GetStopCause() != EAceIteratorStopCause::None)
    {
        return (GetStopCause() == EAceIteratorStopCause::None) ==
            (rhs.GetStopCause() == EAceIteratorStopCause::None);
    }

    auto underlyingEqual = std::visit(
        TOverloaded{
            [&] (const TAclInheritanceIterator& underlyingLhs,
                const TAclInheritanceIterator& underlyingRhs)
            {
                return underlyingLhs == underlyingRhs;
            },
            [&] (const TAclInheritanceIterator& underlyingLhs,
                const TOverriddenBeginAclInheritanceIterator& underlyingRhs)
            {
                return underlyingLhs == underlyingRhs;
            },
            [&] (const TOverriddenBeginAclInheritanceIterator& underlyingLhs,
                const TAclInheritanceIterator& underlyingRhs)
            {
                return underlyingLhs == underlyingRhs;
            },
            [&] (const TOverriddenBeginAclInheritanceIterator& underlyingLhs,
                const TOverriddenBeginAclInheritanceIterator& underlyingRhs)
            {
                return underlyingLhs == underlyingRhs;
            }
        },
        Underlying_,
        rhs.Underlying_);

    if (!underlyingEqual) {
        return false;
    }

    return AclIterator_ == rhs.AclIterator_;
}

TAceIterator::TValue TAceIterator::operator*() const
{
    auto underlyingValue = DereferenceUnderlying();
    return TValue{
        .Object = underlyingValue.Object,
        .Ace = &*AclIterator_,
        .ObjectsTraversed = ObjectsTraversed_
    };
}

////////////////////////////////////////////////////////////////////////////////

TTagFilteringAceIterator::TTagFilteringAceIterator(
    const NObjectServer::IObjectManager* objectManager,
    NObjectServer::TObject* object,
    const TBooleanFormulaTags* tags,
    TAcdOverride firstObjectAcdOverride)
    : Tags_(tags)
    , Underlying_(objectManager, object, std::move(firstObjectAcdOverride))
{ }

TTagFilteringAceIterator& TTagFilteringAceIterator::operator++()
{
    do {
        ++Underlying_;

        if (Underlying_.GetStopCause() != EAceIteratorStopCause::None) {
            break;
        }

        if (!Tags_) {
            break;
        }

        auto underlyingValue = *Underlying_;
        const auto& optionalTagFilter = underlyingValue.Ace->SubjectTagFilter;
        if (!optionalTagFilter) {
            break;
        }

        if (optionalTagFilter->IsSatisfiedBy(*Tags_)) {
            break;
        }
    } while (true);

    return *this;
}

bool TTagFilteringAceIterator::operator==(const TTagFilteringAceIterator& rhs) const
{
    return Underlying_ == rhs.Underlying_;
}

TTagFilteringAceIterator::TValue TTagFilteringAceIterator::operator*() const
{
    return *Underlying_;
}

EAceIteratorStopCause TTagFilteringAceIterator::GetStopCause() const
{
    return Underlying_.GetStopCause();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
