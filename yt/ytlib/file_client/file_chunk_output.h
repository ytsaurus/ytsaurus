#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/api/public.h>

#include <server/chunk_server/public.h>

#include <server/object_server/public.h>

#include <core/compression/codec.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Finish.
 */
class TFileChunkOutput
    : public TOutputStream
{
public:
    //! Initializes an instance.
    TFileChunkOutput(
        NApi::TFileWriterConfigPtr config,
        NChunkClient::TMultiChunkWriterOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        const NObjectClient::TTransactionId& transactionId);

    ~TFileChunkOutput() throw();

    void Open();
    NChunkClient::TChunkId GetChunkId() const;
    i64 GetSize() const;

private:
    //! Adds another portion of data.
    /*!
     *  This portion does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see #TConfig::BlockSize).
     */
    void DoWrite(const void* buf, size_t len);

    //! Closes the writer.
    void DoFinish();
    void FlushBlock();

    const NApi::TFileWriterConfigPtr Config;
    const NChunkClient::TMultiChunkWriterOptionsPtr Options;

    bool IsOpen;

    NRpc::IChannelPtr MasterChannel;
    NObjectClient::TTransactionId TransactionId;

    NChunkClient::TChunkId ChunkId;

    std::vector<NChunkClient::TChunkReplica> Replicas;

    NChunkClient::IChunkWriterPtr ChunkWriter;
    TFileChunkWriterPtr Writer;
    NLog::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
