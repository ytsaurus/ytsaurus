#pragma once

#include "common.h"

#include "../misc/codec.h"
#include "../misc/configurable.h"
#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_rpc.h"
#include "../chunk_client/sequential_reader.h"
#include "../chunk_client/remote_reader.h"
#include "../logging/tagged_logger.h"

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFileReader> TPtr;

    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;
    
        TDuration MasterRpcTimeout;
        NChunkClient::TSequentialReader::TConfig::TPtr SequentialReader;
        NChunkClient::TRemoteReader::TConfig::TPtr RemoteReader;

        TConfig()
        {
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("sequential_reader", SequentialReader).DefaultNew();
            Register("remote_reader", RemoteReader).DefaultNew();
        }
    };

    TFileReader(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        const NYTree::TYPath& path);

    //! Opens the reader. Call this before any other calls.
    void Open();

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Reads the next block.
    /*!
     *  \returns The next block or NULL reference is the end is reached.
     */
    TSharedRef Read();

    //! Closes the reader.
    void Close();

private:
    TConfig::TPtr Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NYTree::TYPath Path;
    bool Closed;
    volatile bool Aborted;

    TAutoPtr<NCypress::TCypressServiceProxy> CypressProxy;
    NChunkClient::TSequentialReader::TPtr SequentialReader;
    NChunkClient::TChunkId ChunkId;
    i32 BlockCount;
    i32 BlockIndex;
    i64 Size;
    ICodec* Codec;

    IAction::TPtr OnAborted_;

    NLog::TTaggedLogger Logger;

    void CheckAborted();
    void OnAborted();

    void Finish();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
