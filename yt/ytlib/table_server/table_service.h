#pragma once

#include "common.h"
#include "table_service_rpc.h"
#include "table_manager.h"

#include "../rpc/server.h"
#include "../meta_state/meta_state_service.h"
#include "../transaction_server/transaction_manager.h"
#include "../chunk_server/chunk_manager.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TTableService> TPtr;

    //! Creates an instance.
    TTableService(
        NMetaState::TMetaStateManager* metaStateManager,
        NChunkServer::TChunkManager* chunkManager,
        TTableManager* tableManager,
        NRpc::IServer* server);

private:
    typedef TTableService TThis;
    typedef TTableServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TServiceException TServiceException;

    NChunkServer::TChunkManager::TPtr ChunkManager;
    TTableManager::TPtr TableManager;

    RPC_SERVICE_METHOD_DECL(NProto, AddTableChunks);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

