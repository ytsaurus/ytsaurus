#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

#include <core/concurrency/action_queue.h>

#include <ytlib/query_client/query_service_proxy.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentService
    : public NRpc::TServiceBase
{
public:
    TQueryAgentService(
        TQueryAgentConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

private:
    typedef TQueryAgentService TThis;
    typedef NQueryClient::TQueryServiceProxy TProxy;

    TQueryAgentConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    NConcurrency::TThreadPoolPtr WorkerPool;

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
