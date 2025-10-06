#pragma once

#include <yt/yt/ytlib/api/native/client.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaReign,
    ((InitialReign)                         (1))
    ((TransientCypressProxyRegistration)    (2))
    ((WritePrerequisiteRevisions)           (3))
    ((UserDirectoryGroups)                  (4))
    ((PermissionValidation)                 (5))
    ((ReadPrerequisiteRevisions)            (6))
    ((TransactionFinisher)                  (7))
    ((InheritableAttributes)                (8))
);

static_assert(TEnumTraits<ESequoiaReign>::IsMonotonic, "Sequoia reign enum is not monotonic");

ESequoiaReign GetCurrentSequoiaReign() noexcept;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGroundReign,
    ((Unknown)          (0))
    ((InitialVersion)   (1))
);

static_assert(TEnumTraits<EGroundReign>::IsMonotonic, "Ground reign enum is not monotonic");

EGroundReign GetCurrentGroundReign();

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ValidateClusterGroundReign(const NApi::NNative::IClientPtr& client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
