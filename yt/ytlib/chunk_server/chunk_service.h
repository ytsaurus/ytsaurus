#pragma once

#include "public.h"
#include "chunk_service_proxy.h"

#include <ytlib/rpc/service.h>

#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NRpc::TServiceBase
{
public:
    explicit TChunkService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

    NCellMaster::TBootstrap* Bootstrap;

    void ValidateNodeId(TNodeId nodeId) const;
    void ValidateTransactionId(const TTransactionId& transactionId) const;
    void CheckAuthorization(const Stroka& address) const;

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterNode);
    DECLARE_RPC_SERVICE_METHOD(NProto, FullHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, IncrementalHeartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
