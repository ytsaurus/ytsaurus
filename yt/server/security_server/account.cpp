#include "stdafx.h"
#include "account.h"

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(const TAccountId& id)
    : TObjectWithIdBase(id)
    , ResourceUsage_(ZeroClusterResources())
    , NodeCount_(0)
{ }

void TAccount::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    ::Save(output, Name_);
    NSecurityServer::Save(output, ResourceUsage_);
    ::Save(output, NodeCount_);
}

void TAccount::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, Name_);
    NSecurityServer::Load(input, ResourceUsage_);
    ::Load(input, NodeCount_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

