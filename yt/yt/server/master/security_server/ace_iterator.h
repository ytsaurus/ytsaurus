#pragma once

#include "acl.h"


namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAceIteratorStopCause,
    ((Undefined)                (0))
    ((HasNoParent)              (1))
    ((InheritanceDisabled)      (2))
);

////////////////////////////////////////////////////////////////////////////////

struct TAcdOverride
{
    std::optional<std::vector<TAccessControlEntry>> Acl;
    std::optional<bool> InheritAcl;
    std::optional<TSubject*> Owner;
};

////////////////////////////////////////////////////////////////////////////////

class TAceIterator
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TSubject*, Owner, nullptr);
    DEFINE_BYVAL_RO_PROPERTY(NObjectServer::TObject*, Object, nullptr);
    DEFINE_BYVAL_RO_PROPERTY(int, Depth, 0);
    DEFINE_BYVAL_RO_PROPERTY(EAceIteratorStopCause, StopCause, EAceIteratorStopCause::Undefined);

public:
    TAceIterator(
        const NObjectServer::IObjectManagerPtr& objectManager,
        NObjectServer::TObject* object,
        TAcdOverride firstObjectOverride = {});

    TAceIterator& operator++();

    Y_FORCE_INLINE bool operator==(const TAceIterator& another) noexcept;

    Y_FORCE_INLINE const TAccessControlEntry& operator*() noexcept;

    static TAceIterator End();

private:
    TAceIterator() = default;

    const NObjectServer::IObjectManager* const ObjectManager_ = nullptr;

    const TAccessControlEntry* Ace_ = nullptr;
    const TAccessControlEntry* AclEnd_ = nullptr;
    TAccessControlDescriptor* Acd_ = nullptr;
    NObjectServer::IObjectTypeHandlerPtr Handler_;

    TAcdOverride FirstObjectAcdOverride_;

    void GoToNextObject();
    void BindToObject(NObjectServer::TObject* object);
    void GoToFirstNonEmptyAncestor();
    void FinalizeAcdIteration(EAceIteratorStopCause cause) noexcept;
    bool AcquireInheritanceModifier() noexcept;
    Y_FORCE_INLINE bool IsIterationFinished() noexcept;
};

} // namespace NYT::NSecurityServer

#define ACE_ITERATOR_INL_H_
#include "ace_iterator-inl.h"
#undef ACE_ITERATOR_INL_H_
