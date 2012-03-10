#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_service_proxy.h"

#include <ytlib/cell_master/meta_state_service.h>
#include <ytlib/rpc/server.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TChunkService> TPtr;

    //! Creates an instance.
    TChunkService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

    void ValidateHolderId(THolderId holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterHolder);
    DECLARE_RPC_SERVICE_METHOD(NProto, FullHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, IncrementalHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, CreateChunks);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
