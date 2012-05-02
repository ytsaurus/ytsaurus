#pragma once

#include "public.h"

#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/chunk_client/remote_writer.h>

namespace NYT {
namespace NFileClient {
    
////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TFileWriterBase
    : public NTransactionClient::TTransactionListener
{
public:
    //! Initializes an instance.
    TFileWriterBase(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel);

    //! Opens the writer.
    void Open(NObjectServer::TTransactionId);

    //! Adds another portion of data.
    /*!
     *  This portion does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see #TConfig::BlockSize).
     */
    void Write(TRef data);

    //! Cancels the writing process and releases all resources.
    void Cancel();

    //! Closes the writer.
    void Close();

protected:
    NRpc::IChannelPtr MasterChannel;
    NLog::TTaggedLogger Logger;

    virtual void DoClose(const NChunkServer::TChunkId& chunkId);

private:
    TFileWriterConfigPtr Config;
    bool IsOpen;
    i64 Size;
    i32 BlockCount;
    NChunkClient::TRemoteWriterPtr Writer;
    NChunkServer::TChunkId ChunkId;
    ICodec* Codec;
    TBlob Buffer;

    void FlushBlock();

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
