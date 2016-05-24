#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/transaction_server/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateLockProxy(
    NCellMaster::TBootstrap* bootstrap,
    TLock* lock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
