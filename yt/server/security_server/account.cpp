#include "stdafx.h"
#include "account.h"

#include <core/ytree/convert.h>

#include <server/cell_master/serialize.h>

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

void TAccount::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, ResourceUsage_);
    Save(context, CommittedResourceUsage_);
    Save(context, ResourceLimits_);
    Save(context, Acd_);
}

void TAccount::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, ResourceUsage_);
    Load(context, CommittedResourceUsage_);
    Load(context, ResourceLimits_);
    Load(context, Acd_);
}

bool TAccount::IsOverDiskSpaceLimit() const
{
    return ResourceUsage_.DiskSpace > ResourceLimits_.DiskSpace;
}

void TAccount::ValidateDiskSpaceLimit() const
{
    if (IsOverDiskSpaceLimit()) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over disk space limit",
            Name_)
            << TErrorAttribute("usage", ResourceUsage_.DiskSpace)
            << TErrorAttribute("limit", ResourceLimits_.DiskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

