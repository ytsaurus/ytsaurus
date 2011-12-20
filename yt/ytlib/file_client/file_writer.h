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
        NChunkClient::TRemoteWriter::TConfig::TPtr RemoteWriter;

        TConfig()
        {
            Register("block_size", BlockSize).Default(1024 * 1024).GreaterThan(0);
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("codec_id", CodecId).Default(ECodecId::None);
            Register("remote_writer", RemoteWriter).Default(New<NChunkClient::TRemoteWriter::TConfig>());
        }
    };

    TFileWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        const NYTree::TYPath& path,
        int totalReplicaCount = 3,
        int uploadReplicaCount = 2);

    void Write(TRef data);

    void Cancel();

    void Close();

private:
    TConfig::TPtr Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
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
