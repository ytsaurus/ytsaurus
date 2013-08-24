#pragma once

#include "public.h"

#include <server/transaction_server/public.h>

#include <server/object_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateLockProxy(
    NCellMaster::TBootstrap* bootstrap,
    TLock* lock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
