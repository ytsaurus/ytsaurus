#pragma once

#include "acl.h"

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAceIteratorStopCause,
    ((None)                     (0))
    ((NoParent)                 (1))
    ((InheritanceDisabled)      (2))
);

////////////////////////////////////////////////////////////////////////////////

//! A device for iterating over a chain of object's ancestors (in the sense of
//! ACL inheritance).
/*!
 *  The iterator stops after either exhausting ancestors or reaching inheritance
 *  break point (marked by inherit_acl == false). This can be checked by
 *  #GetStopCause or, as usual, by comparing to an end iterator.
 *
 *  The iterator may yield nulls (for Acl and InheritAcl) if an ancestor has no
 *  associated ACD. This should not be treated as reaching an end.
 */
class TAclInheritanceIterator
{
public:
    TAclInheritanceIterator(
        const NObjectServer::IObjectManager* objectManager,
        NObjectServer::TObject* begin);

    //! Constructs an end iterator.
    TAclInheritanceIterator() = default;

    struct TValue
    {
        NObjectServer::TObject* Object;

        // Any of these may be null if the object has no associated ACD.
        const TAccessControlList* Acl;
        const bool* InheritAcl;
    };

    TAclInheritanceIterator& operator++();

    bool operator==(const TAclInheritanceIterator& rhs) const;

    TValue operator*() const;

    EAceIteratorStopCause GetStopCause() const;

    TAclInheritanceIterator& IncrementIngoreInheritAcl();

private:
    const NObjectServer::IObjectManager* ObjectManager_ = nullptr;
    NObjectServer::TObject* Object_ = nullptr;
    // Just a cache.
    NObjectServer::IObjectTypeHandler* ObjectTypeHandler_ = nullptr;

    void OnObjectUpdated();
    void MarkAsEnd();
    void UpdateTypeHandler();

    TAclInheritanceIterator& DoIncrement(bool ingoreInheritAcl = false);
};

////////////////////////////////////////////////////////////////////////////////

struct TAcdOverride
{
    TAcdOverride() = default;
    explicit TAcdOverride(TAccessControlList acl);
    explicit TAcdOverride(bool inheritAcl);
    explicit TAcdOverride(TSubject& owner);

    TAcdOverride(TAcdOverride&&) = default;
    TAcdOverride(const TAcdOverride&) = default;
    TAcdOverride& operator=(TAcdOverride&&) = default;
    TAcdOverride& operator=(const TAcdOverride&) = default;

    void Reset();

    explicit operator bool() const;

    bool operator==(const TAcdOverride& other) const = default;

    DEFINE_BYREF_RO_PROPERTY(std::optional<TAccessControlList>, Acl);
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, InheritAcl);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TSubject*>, Owner);
};

////////////////////////////////////////////////////////////////////////////////

//! Same as #TAclInheritanceIterator but supports overriding the starting
//! object's ACD. Useful for validating a permission change prior to applying it.
class TOverriddenBeginAclInheritanceIterator
{
public:
    using TValue = TAclInheritanceIterator::TValue;
    using TThis = TOverriddenBeginAclInheritanceIterator;

    // #beginOverride must not be empty (i.e. it must override something).
    // Otherwise, consider using #TAclInheritanceIterator instead.
    TOverriddenBeginAclInheritanceIterator(
        const NObjectServer::IObjectManager* objectManager,
        NObjectServer::TObject* begin,
        TAcdOverride beginOverride);

    //! Constructs an end iterator.
    TOverriddenBeginAclInheritanceIterator() = default;

    TThis& operator++();

    bool operator==(const TThis& rhs) const;
    bool operator==(const TAclInheritanceIterator& rhs) const;

    TValue operator*() const;

    EAceIteratorStopCause GetStopCause() const;

private:
    TAcdOverride BeginOverride_;
    TAclInheritanceIterator Underlying_;

    bool HasOverride() const;
    void ResetOverride();
    TValue GetOveriddenValue() const;
};

////////////////////////////////////////////////////////////////////////////////

//! A device for iterating over entries (ACEs) of an object's effective ACL.
//! Supports overriding ACD of the starting object.
class TAceIterator
{
public:
    struct TValue
    {
        NObjectServer::TObject* Object;
        const TAccessControlEntry* Ace;
        int ObjectsTraversed;
    };

    TAceIterator(
        const NObjectServer::IObjectManager* objectManager,
        NObjectServer::TObject* object,
        TAcdOverride firstObjectAcdOverride = {});

    // Constructs and end iterator.
    TAceIterator() = default;

    TAceIterator& operator++();

    bool operator==(const TAceIterator& rhs) const;

    TValue operator*() const;

    EAceIteratorStopCause GetStopCause() const;

private:
    std::variant<
        TAclInheritanceIterator,
        TOverriddenBeginAclInheritanceIterator
    > Underlying_;
    TAccessControlList::TEntries::const_iterator AclIterator_;
    int ObjectsTraversed_ = 0;

    TAclInheritanceIterator::TValue DereferenceUnderlying() const;
    void SkipNullUnderlying();
    void AdvanceUnderlying();
    void DoAdvanceUnderlying(bool checkFirst);

    void UpdateAclIterator();
};

////////////////////////////////////////////////////////////////////////////////

//! Same as TAceIterator but supports attribute-based access control (ABAC).
/*!
 *  Essentially, this iterator is parameterized by an optional set of user tags.
 *  It then skips ACEs whose subject tag filter, if any, is not satisfied by
 *  these tags.
 */
class TTagFilteringAceIterator
{
public:
    using TValue = TAceIterator::TValue;

    TTagFilteringAceIterator(
        const NObjectServer::IObjectManager* objectManager,
        NObjectServer::TObject* object,
        const TBooleanFormulaTags* tags = nullptr,
        TAcdOverride firstObjectAcdOverride = {});

    //! Constructs and end iterator.
    TTagFilteringAceIterator() = default;

    TTagFilteringAceIterator& operator++();

    bool operator==(const TTagFilteringAceIterator& rhs) const;

    TValue operator*() const;

    EAceIteratorStopCause GetStopCause() const;

private:
    const TBooleanFormulaTags* const Tags_ = nullptr;
    TAceIterator Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

