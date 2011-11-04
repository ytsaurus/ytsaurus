#pragma once

#include "common.h"
#include "file_service_rpc.h"
#include "file_manager.h"

#include "../rpc/server.h"
#include "../meta_state/meta_state_service.h"
#include "../transaction_manager/transaction_manager.h"
#include "../chunk_server/chunk_manager.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TFileService> TPtr;

    //! Creates an instance.
    TFileService(
        NMetaState::TMetaStateManager* metaStateManager,
        NChunkServer::TChunkManager* chunkManager,
        TFileManager* fileManager,
        NRpc::TServer* server);

private:
    typedef TFileService TThis;
    typedef TFileServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    NChunkServer::TChunkManager::TPtr ChunkManager;
    TFileManager::TPtr FileManager;

    RPC_SERVICE_METHOD_DECL(NProto, SetFileChunk);
    RPC_SERVICE_METHOD_DECL(NProto, GetFileChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

