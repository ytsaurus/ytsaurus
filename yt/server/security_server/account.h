#pragma once

#include "public.h"
#include "cluster_resources.h"
#include "acl.h"

#include <core/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Name);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, CommittedResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    explicit TAccount(const TAccountId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    //! Returns |true| if disk space limit is exceeded,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated() const;

    //! Returns |true| is node count limit is exceeded,
    //! i.e. no more Cypress node could be created.
    bool IsNodeCountLimitViolated() const;

    //! Returns |true| is chunk count limit is exceeded,
    //! i.e. no more chunks could be created.
    bool IsChunkCountLimitViolated() const;

    //! Throws if account limit is exceeded for some resource type with positive delta.
    void ValidateResourceUsageIncrease(const TClusterResources& delta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
