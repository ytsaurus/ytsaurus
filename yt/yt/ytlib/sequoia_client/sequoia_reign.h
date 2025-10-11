#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaReign,
    ((InitialReign)                        ( 1))
    ((TransientCypressProxyRegistration)   ( 2))
    ((WritePrerequisiteRevisions)          ( 3))
    ((UserDirectoryGroups)                 ( 4))
    ((PermissionValidation)                ( 5))
    ((ReadPrerequisiteRevisions)           ( 6))
    ((TransactionFinisher)                 ( 7))
    ((InheritableAttributes)               ( 8))
    ((FixAccountInheritance)               ( 9))
    ((FixAccountInCopy)                    (10))
);

static_assert(TEnumTraits<ESequoiaReign>::IsMonotonic, "Sequoia reign enum is not monotonic");

ESequoiaReign GetCurrentSequoiaReign() noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
