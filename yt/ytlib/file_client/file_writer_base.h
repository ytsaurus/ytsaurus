#pragma once

#include "public.h"

#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/object_server/public.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/rpc/public.h>

namespace NYT {
namespace NFileClient {
    
////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TFileWriterBase
    : public virtual TRefCounted
{
public:
    //! Initializes an instance.
    TFileWriterBase(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NObjectServer::TTransactionId transactionId);

    virtual ~TFileWriterBase();

    //! Opens the writer.
    virtual void Open();

    //! Adds another portion of data.
    /*!
     *  This portion does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see #TConfig::BlockSize).
     */
    virtual void Write(TRef data);

    //! Closes the writer.
    virtual void Close();

protected:
    TFileWriterConfigPtr Config;

    NRpc::IChannelPtr MasterChannel;
    NObjectServer::TTransactionId TransactionId;

    bool IsOpen;
    i64 Size;
    i32 BlockCount;
    NChunkClient::TRemoteWriterPtr Writer;
    NChunkServer::TChunkId ChunkId;
    ICodec* Codec;
    TBlob Buffer;
    NChunkHolder::NProto::TChunkMeta Meta;

    NLog::TTaggedLogger Logger;

    void FlushBlock();

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
