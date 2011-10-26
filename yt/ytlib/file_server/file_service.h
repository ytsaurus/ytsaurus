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

using NCypress::TCypressManager;
using NTransaction::TTransactionManager;
using NChunkServer::TChunkManager;

////////////////////////////////////////////////////////////////////////////////

class TFileService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TFileService> TPtr;

    //! Creates an instance.
    TFileService(
        TCypressManager::TPtr cypressManager,
        TTransactionManager::TPtr transactionManager,
        TChunkManager::TPtr chunkManager,
        TFileManager::TPtr fileManager,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server);

private:
    typedef TFileService TThis;
    typedef TFileServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    TCypressManager::TPtr CypressManager;
    TTransactionManager::TPtr TransactionManager;
    TChunkManager::TPtr ChunkManager;
    TFileManager::TPtr FileManager;

    void RegisterMethods();

    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateNodeId(const TNodeId& nodeId, const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId);

    RPC_SERVICE_METHOD_DECL(NProto, SetFileChunk);
    RPC_SERVICE_METHOD_DECL(NProto, GetFileChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

