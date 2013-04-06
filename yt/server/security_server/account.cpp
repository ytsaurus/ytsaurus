#include "stdafx.h"
#include "account.h"

#include <ytlib/ytree/convert.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(const TAccountId& id)
    : TNonversionedObjectBase(id)
    , ResourceUsage_(ZeroClusterResources())
    , CommittedResourceUsage_(ZeroClusterResources())
    , ResourceLimits_(ZeroClusterResources())
    , Acd_(this)
{ }

void TAccount::Save(const NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    NCellMaster::Save(context, Name_);
    NSecurityServer::Save(context, ResourceUsage_);
    NSecurityServer::Save(context, CommittedResourceUsage_);
    NSecurityServer::Save(context, ResourceLimits_);
    NSecurityServer::Save(context, Acd_);
}

void TAccount::Load(const NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    NCellMaster::Load(context, Name_);
    NSecurityServer::Load(context, ResourceUsage_);
    NSecurityServer::Load(context, CommittedResourceUsage_);
    NSecurityServer::Load(context, ResourceLimits_);
    NSecurityServer::Load(context, Acd_);
}

bool TAccount::IsOverDiskSpaceLimit() const
{
    return ResourceUsage_.DiskSpace > ResourceLimits_.DiskSpace;
}

void TAccount::ValidateDiskSpaceLimit() const
{
    if (IsOverDiskSpaceLimit()) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountIsOverLimit,
            "Account is over disk space limit: %s",
            ~Name_)
            << TErrorAttribute("usage", ResourceUsage_.DiskSpace)
            << TErrorAttribute("limit", ResourceLimits_.DiskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

