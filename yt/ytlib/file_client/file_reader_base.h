#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/rpc/channel.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/ref.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
class TFileReaderBase
    : public TNonCopyable
{
public:
    //! Initializes an instance.
    TFileReaderBase();

    //! Opens the reader.
    void Open(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NChunkClient::TNodeDirectoryPtr nodeDirectory,
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::TChunkReplicaList& replicas,
        const TNullable<i64>& offset,
        const TNullable<i64>& length);

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Reads the next block.
    /*!
     *  \returns The next block or NULL reference is the end is reached.
     */
    TSharedRef Read();

private:
    bool IsOpen;
    i32 BlockIndex;

    NChunkClient::TSequentialReaderPtr SequentialReader;
    i64 Size;

    i64 StartOffset;
    i64 EndOffset;

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
