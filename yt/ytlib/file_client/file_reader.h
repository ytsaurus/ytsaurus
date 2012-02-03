#pragma once

#include "common.h"

//#include <ytlib/misc/thread_affinity.h>
//#include <ytlib/misc/codec.h>
//#include <ytlib/misc/configurable.h>
//#include <ytlib/rpc/channel.h>
//#include <ytlib/transaction_client/transaction.h>
//#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/file_client/file_reader_base.h>
#include <ytlib/cypress/cypress_service_proxy.h>
//#include <ytlib/chunk_client/sequential_reader.h>
//#include <ytlib/chunk_client/block_cache.h>
//#include <ytlib/chunk_client/remote_reader.h>
//#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
class TFileReader
    : public TFileReaderBase
{
public:
    typedef TIntrusivePtr<TFileReader> TPtr;

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

private:
    NTransactionClient::ITransaction::TPtr Transaction;
    // NTransactionClient::TTransactionId TransactionId;
    // NChunkClient::IBlockCache::TPtr BlockCache;
    NYTree::TYPath Path;

    // NChunkClient::TSequentialReader::TPtr SequentialReader;
    // i64 Size;
    // ICodec* Codec;
    Stroka FileName;
    bool Executable;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
