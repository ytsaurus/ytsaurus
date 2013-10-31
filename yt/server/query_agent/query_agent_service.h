#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/query_client/query_service_proxy.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentService
    : public NRpc::TServiceBase
{
public:
    explicit TQueryAgentService(NCellNode::TBootstrap* bootstrap);

private:
    typedef TQueryAgentService TThis;
    typedef NQueryClient::TQueryServiceProxy TProxy;

    NVersionedTableClient::TChunkWriterConfigPtr ChunkWriterConfig_;
    NChunkClient::TEncodingWriterOptionsPtr EncodingWriterOptions_;

    NCellNode::TBootstrap* Bootstrap;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
