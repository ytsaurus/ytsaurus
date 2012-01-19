#pragma once

#include "common.h"

#include <ytlib/misc/codec.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFileReader> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;
    
        TDuration MasterRpcTimeout;
        NChunkClient::TSequentialReader::TConfig::TPtr SequentialReader;
        NChunkClient::TRemoteReaderConfig::TPtr RemoteReader;

        TConfig()
        {
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("sequential_reader", SequentialReader).DefaultNew();
            Register("remote_reader", RemoteReader).DefaultNew();
        }
    };

    //! Initializes an instance.
    TFileReader(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NChunkClient::IBlockCache* blockCache,
        const NYTree::TYPath& path);

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Returns the file name (as provided by the master).
    Stroka GetFileName() const;

    //! Returns the executable mode.
    bool IsExecutable();

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
    Stroka FileName;
    bool Executable;

    IAction::TPtr OnAborted_;

    NLog::TTaggedLogger Logger;

    void CheckAborted();
    void OnAborted();

    void Finish();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
