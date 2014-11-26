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

bool TAccount::IsDiskSpaceLimitViolated() const
{
    return ResourceUsage_.DiskSpace > ResourceLimits_.DiskSpace;
}

bool TAccount::IsNodeCountLimitViolated() const
{
    return ResourceUsage_.NodeCount > ResourceLimits_.NodeCount;
}

bool TAccount::IsChunkCountLimitViolated() const
{
    return ResourceUsage_.ChunkCount > ResourceLimits_.ChunkCount;
}

void TAccount::ValidateResourceUsageIncrease(const TClusterResources& delta)
{
    if (delta.DiskSpace > 0 && ResourceUsage_.DiskSpace + delta.DiskSpace > ResourceLimits_.DiskSpace) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over disk space limit",
            Name_)
            << TErrorAttribute("usage", ResourceUsage_.DiskSpace)
            << TErrorAttribute("limit", ResourceLimits_.DiskSpace);
    }
    if (delta.NodeCount > 0 && ResourceUsage_.NodeCount + delta.NodeCount > ResourceLimits_.NodeCount) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over node count limit",
            Name_)
            << TErrorAttribute("usage", ResourceUsage_.NodeCount)
            << TErrorAttribute("limit", ResourceLimits_.NodeCount);
    }
    if (delta.ChunkCount > 0 && ResourceUsage_.ChunkCount + delta.ChunkCount > ResourceLimits_.ChunkCount) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over chunk count limit",
            Name_)
            << TErrorAttribute("usage", ResourceUsage_.ChunkCount)
            << TErrorAttribute("limit", ResourceLimits_.ChunkCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

