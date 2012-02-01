#pragma once

#include "common.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
class TFileReader
    : public NTransactionClient::TTransactionListener
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

    //! Opens the reader.
    void Open();

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Returns the file name (as provided by the master).
    Stroka GetFileName() const;

    //! Returns the executable flag.
    bool IsExecutable();

    //! Reads the next block.
    /*!
     *  \returns The next block or NULL reference is the end is reached.
     */
    TSharedRef Read();

private:
    TConfig::TPtr Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::IBlockCache::TPtr BlockCache;
    NYTree::TYPath Path;
    bool IsOpen;
    i32 BlockCount;
    i32 BlockIndex;
    NCypress::TCypressServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    NChunkClient::TSequentialReader::TPtr SequentialReader;
    i64 Size;
    ICodec* Codec;
    Stroka FileName;
    bool Executable;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
