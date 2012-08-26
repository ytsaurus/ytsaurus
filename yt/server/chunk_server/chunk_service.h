#pragma once

#include "public.h"
#include "chunk_service_proxy.h"

#include <ytlib/rpc/service.h>

#include <server/cell_master/meta_state_service.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TChunkService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

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
