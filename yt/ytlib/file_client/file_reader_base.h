#pragma once

#include "public.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/ref.h>

#include <ytlib/compression/public.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/rpc/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
// TODO(babenko): update doc and rename
class TFileReaderBase
    : public TNonCopyable
{
public:
    TFileReaderBase();

    void Open(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
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
    int BlockIndex;
    i64 Size;
    i64 StartOffset;
    i64 EndOffset;

    NLog::TTaggedLogger Logger;

    NChunkClient::TSequentialReaderPtr SequentialReader;
    NCompression::ICodec* Codec;

    Stroka FileName;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
