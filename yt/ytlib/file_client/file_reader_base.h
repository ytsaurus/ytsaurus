#pragma once

#include "public.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/codecs/codec.h>
#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/object_server/object_service_proxy.h>
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
class TFileReaderBase
    : public NTransactionClient::TTransactionListener
{
public:
    //! Initializes an instance.
    TFileReaderBase(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache);

    //! Opens the reader.
    void Open(
        const NChunkServer::TChunkId& chunkId,
        const std::vector<Stroka>& nodeAddresses);

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Reads the next block.
    /*!
     *  \returns The next block or NULL reference is the end is reached.
     */
    TSharedRef Read();

private:
    TFileReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NChunkClient::IBlockCachePtr BlockCache;
    NYTree::TYPath Path;
    bool IsOpen;
    i32 BlockCount;
    i32 BlockIndex;
protected:
    NObjectServer::TObjectServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

private:
    NChunkClient::TSequentialReaderPtr SequentialReader;
    i64 Size;
    ICodec* Codec;
    Stroka FileName;
    bool Executable;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
