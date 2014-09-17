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

    //! Returns |true| if disk space limit is exceeded.
    bool IsOverDiskSpaceLimit() const;

    //! Throws is disk space limit is exceeded.
    void ValidateDiskSpaceLimit() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
