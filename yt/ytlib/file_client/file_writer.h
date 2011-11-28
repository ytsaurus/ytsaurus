#pragma once

#include "common.h"

#include "../misc/config.h"
#include "../misc/codec.h"
#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_rpc.h"
#include "../chunk_client/remote_writer.h"

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFileWriter> TPtr;

    struct TConfig
        : TConfigBase
    {
        i64 BlockSize;
        TDuration MasterRpcTimeout;
        ECodecId CodecId;
        NChunkClient::TRemoteWriter::TConfig Writer;

        TConfig()
        {
            Register("block_size", BlockSize).Default(1024 * 1024).GreaterThan(0);
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("codec_id", CodecId).Default(ECodecId::None);
            Register("writer", Writer);

            SetDefaults();
        }
    };

    TFileWriter(
        const TConfig& config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NYTree::TYPath path,
        int totalReplicaCount = 3,
        int uploadReplicaCount = 2);

    void Write(TRef data);

    void Cancel();

    void Close();

private:
    TConfig Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NYTree::TYPath Path;
    bool Closed;
    volatile bool Aborted;

    TAutoPtr<NCypress::TCypressServiceProxy> CypressProxy;
    NChunkClient::TRemoteWriter::TPtr Writer;
    NCypress::TNodeId NodeId;
    NChunkClient::TChunkId ChunkId;

    i64 Size;
    i32 BlockCount;
    TBlob Buffer;

    IAction::TPtr OnAborted_;

    void CheckAborted();
    void OnAborted();

    void FlushBlock();
    void Finish();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT