#pragma once

#include <yt/yt/ytlib/api/native/client.h>

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
    ((BannedUsers)                         (11))
    ((FromObjectIdResolve)                 (12))
    ((FromObjectIdResolveLinkTarget)       (13))
    ((AnnotationAttribute)                 (14))
    ((CompositeNodeReadAccessControl)      (15))
    ((ChildNodesTable)                     (16))
);

static_assert(TEnumTraits<ESequoiaReign>::IsMonotonic, "Sequoia reign enum is not monotonic");

ESequoiaReign GetCurrentSequoiaReign() noexcept;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGroundReign,
    ((Unknown)          (0))
    ((InitialVersion)   (1))
    ((ChildNodesTable)  (2))
);

static_assert(TEnumTraits<EGroundReign>::IsMonotonic, "Ground reign enum is not monotonic");

EGroundReign GetCurrentGroundReign();

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ValidateClusterGroundReign(
    const NApi::NNative::IClientPtr& client,
    const NYTree::TYPath& rootPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
