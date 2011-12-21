#pragma once

#include "common.h"

#include "../misc/configurable.h"
#include "../misc/codec.h"
#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_rpc.h"
#include "../chunk_client/remote_writer.h"
#include "../chunk_server/chunk_service_rpc.h"
#include "../logging/tagged_logger.h"

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFileWriter> TPtr;

    struct TConfig
        : TConfigurable
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
            Register("block_size", BlockSize).Default(1024 * 1024).GreaterThan(0);
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("codec_id", CodecId).Default(ECodecId::None);
            Register("total_replica_count", TotalReplicaCount).Default(3).GreaterThanOrEqual(1);
            Register("upload_replica_count", UploadReplicaCount).Default(2).GreaterThanOrEqual(1);
            Register("remote_writer", RemoteWriter).DefaultNew();
        }

        virtual void Validate(const NYTree::TYPath& path = "/")
        {
            TConfigurable::Validate(path);
            
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
        const NYTree::TYPath& path);

    //! Adds another chunk of data.
    /*!
     *  This chunk does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see TConfig::BlockSize).
     */
    void Write(TRef data);

    //! Cancels the writing process releasing all resources.
    void Cancel();

    //! Closes the writer.
    void Close();

private:
    TConfig::TPtr Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionServer::TTransactionId TransactionId;
    NYTree::TYPath Path;
    bool Closed;
    volatile bool Aborted;

    TAutoPtr<NCypress::TCypressServiceProxy> CypressProxy;
    TAutoPtr<NChunkServer::TChunkServiceProxy> ChunkProxy;

    NChunkClient::TRemoteWriter::TPtr Writer;
    NCypress::TNodeId NodeId;
    NChunkClient::TChunkId ChunkId;
    ICodec* Codec;

    i64 Size;
    i32 BlockCount;
    TBlob Buffer;

    IAction::TPtr OnAborted_;

    NLog::TTaggedLogger Logger;

    void CheckAborted();
    void OnAborted();

    void FlushBlock();
    void Finish();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
