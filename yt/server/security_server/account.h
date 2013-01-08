#pragma once

#include "public.h"
#include "cluster_resources.h"

#include <ytlib/misc/property.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public NObjectServer::TUnversionedObjectBase
{
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Name);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(int, NodeCount);

public:
    explicit TAccount(const TAccountId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    bool IsOverDiskSpace() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
