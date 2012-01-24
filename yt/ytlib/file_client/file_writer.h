#pragma once

#include "common.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/cypress/id.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>

namespace NYT {
namespace NFileClient {
    
////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TFileWriter
    : public NTransactionClient::TTransactionListener
{
public:
    typedef TIntrusivePtr<TFileWriter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        i64 BlockSize;
        TDuration MasterRpcTimeout;
        ECodecId CodecId;
        int TotalReplicaCount;
        int UploadReplicaCount;
        NChunkClient::TRemoteWriter::TConfig::TPtr RemoteWriter;

        TConfig()
        {
            Register("block_size", BlockSize)
                .Default(1024 * 1024)
                .GreaterThan(0);
            Register("master_rpc_timeout", MasterRpcTimeout).
                Default(TDuration::MilliSeconds(5000));
            Register("codec_id", CodecId)
                .Default(ECodecId::None);
            Register("total_replica_count", TotalReplicaCount)
                .Default(3)
                .GreaterThanOrEqual(1);
            Register("upload_replica_count", UploadReplicaCount)
                .Default(2)
                .GreaterThanOrEqual(1);
            Register("remote_writer", RemoteWriter)
                .DefaultNew();
        }

        virtual void DoValidate()
        {
            if (TotalReplicaCount < UploadReplicaCount) {
                ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
            }
        }
    };

    //! Initializes an instance.
    TFileWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NTransactionClient::TTransactionManager* transactionManager,
        const NYTree::TYPath& path);

    //! Opens the writer.
    void Open();

    //! Adds another portion of data.
    /*!
     *  This portion does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see #TConfig::BlockSize).
     */
    void Write(TRef data);

    //! Cancels the writing process and releases all resources.
    void Cancel();

    //! Closes the writer.
    void Close();

private:
    TConfig::TPtr Config;
    NTransactionClient::ITransaction::TPtr Transaction;
    NObjectServer::TTransactionId TransactionId;
    NTransactionClient::ITransaction::TPtr UploadTransaction;
    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NYTree::TYPath Path;
    bool IsOpen;
    bool IsClosed;
    i64 Size;
    i32 BlockCount;
    NCypress::TCypressServiceProxy CypressProxy;
    NChunkServer::TChunkServiceProxy ChunkProxy;
    NLog::TTaggedLogger Logger;

    NChunkClient::TRemoteWriter::TPtr Writer;
    NCypress::TNodeId NodeId;
    NChunkServer::TChunkId ChunkId;
    ICodec* Codec;
    TBlob Buffer;

    void FlushBlock();

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
