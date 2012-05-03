#pragma once

#include "common.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
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
    typedef TIntrusivePtr<TFileWriterBase> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        i64 BlockSize;
        ECodecId CodecId;
        int ReplicationFactor;
        int UploadReplicationFactor;
        NChunkClient::TRemoteWriter::TConfig::TPtr RemoteWriter;

        TConfig()
        {
            Register("block_size", BlockSize)
                .Default(1024 * 1024)
                .GreaterThan(0);
            Register("codec_id", CodecId)
                .Default(ECodecId::None);
            Register("replication_factor", ReplicationFactor)
                .Default(3)
                .GreaterThanOrEqual(1);
            Register("upload_replication_factor", UploadReplicationFactor)
                .Default(2)
                .GreaterThanOrEqual(1);
            Register("remote_writer", RemoteWriter)
                .DefaultNew();
        }

        virtual void DoValidate()
        {
            if (ReplicationFactor < UploadReplicationFactor) {
                ythrow yexception() << "\"replication_factor\" cannot be less than \"upload_replication_factor\"";
            }
        }
    };

    //! Initializes an instance.
    TFileWriterBase(
        TConfig::TPtr config,
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
    TConfig::TPtr Config;
    bool IsOpen;
    i64 Size;
    i32 BlockCount;
    NChunkClient::TRemoteWriter::TPtr Writer;
    NChunkServer::TChunkId ChunkId;
    ICodec* Codec;
    TBlob Buffer;

    void FlushBlock();

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
