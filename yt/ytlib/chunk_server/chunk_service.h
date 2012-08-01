#pragma once

#include "public.h"
#include "chunk_service_proxy.h"

#include <ytlib/cell_master/meta_state_service.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    //! Creates an instance.
    TChunkService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

    void ValidateNodeId(TNodeId nodeId);
    void ValidateTransactionId(const TTransactionId& transactionId);

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterNode);
    DECLARE_RPC_SERVICE_METHOD(NProto, FullHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, IncrementalHeartbeat);

    void CheckAuthorization(const Stroka& address) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
