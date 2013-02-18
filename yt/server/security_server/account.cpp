#include "stdafx.h"
#include "account.h"

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(const TAccountId& id)
    : TUnversionedObjectBase(id)
    , ResourceUsage_(ZeroClusterResources())
    , CommittedResourceUsage_(ZeroClusterResources())
    , ResourceLimits_(ZeroClusterResources())
    , NodeCount_(0)
{ }

void TAccount::Save(const NCellMaster::TSaveContext& context) const
{
    TUnversionedObjectBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, Name_);
    NSecurityServer::Save(output, ResourceUsage_);
    NSecurityServer::Save(output, CommittedResourceUsage_);
    NSecurityServer::Save(output, ResourceLimits_);
    ::Save(output, NodeCount_);
}

void TAccount::Load(const NCellMaster::TLoadContext& context)
{
    TUnversionedObjectBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, Name_);
    NSecurityServer::Load(input, ResourceUsage_);
    NSecurityServer::Load(input, CommittedResourceUsage_);
    NSecurityServer::Load(input, ResourceLimits_);
    ::Load(input, NodeCount_);
}

bool TAccount::IsOverDiskSpace() const
{
    return ResourceUsage_.DiskSpace > ResourceLimits_.DiskSpace;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

