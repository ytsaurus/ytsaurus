#pragma once

#include <ytlib/rpc/service.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

NRpc::IService::TPtr CreateRedirectorService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
