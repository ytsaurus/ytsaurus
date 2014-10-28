#pragma once

#include "public.h"
#include "config.h"
#include "chunk_replica.h"
#include "writer_base.h"

#include <core/misc/async_stream_state.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TProvider>
class TOldMultiChunkSequentialWriter
    : public virtual IWriterBase
{
public:
    typedef TIntrusivePtr<TProvider> TProviderPtr;
    typedef typename TProvider::TChunkWriter TChunkWriter;
    typedef typename TProvider::TFacade TFacade;

    TOldMultiChunkSequentialWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        TProviderPtr provider,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TChunkListId& parentChunkListId);

    virtual TAsyncError Open() override;
    virtual TAsyncError Close() override;

    // Returns pointer to writer facade, which allows single write operation.
    // In nullptr is returned, caller should subscribe to ReadyEvent.
    TFacade* GetCurrentWriter();
    virtual TAsyncError GetReadyEvent() override;

    void SetProgress(double progress);

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NChunkClient::NProto::TChunkSpec>& GetWrittenChunks() const;

    //! Provides node id to descriptor mapping for chunks returned via #GetWrittenChunks.
    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;
    TProviderPtr GetProvider();

protected:
    struct TSession
    {
        TIntrusivePtr<TChunkWriter> ChunkWriter;
        IChunkWriterPtr AsyncWriter;
        TChunkId ChunkId;

        TSession()
            : ChunkWriter(nullptr)
            , AsyncWriter(nullptr)
        { }

        bool IsNull() const
        {
            return !ChunkWriter;
        }

        void Reset()
        {
            ChunkWriter.Reset();
            AsyncWriter.Reset();
            ChunkId = TChunkId();
        }
    };

    void CreateNextSession();
    void InitCurrentSession(TSession nextSession);

    void OnChunkCreated(NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr rsp);

    TAsyncError FinishCurrentSession();

    void OnChunkClosed(
        int chunkIndex,
        TSession currentSession,
        TAsyncErrorPromise finishResult,
        TError error);

    void OnChunkConfirmed(
        TChunkId chunkId,
        TAsyncErrorPromise finishResult,
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    void OnChunkFinished(
        TChunkId chunkId,
        TError error);

    void OnRowWritten();

    void AttachChunks();
    void OnClose(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    void SwitchSession();

    const TMultiChunkWriterConfigPtr Config;
    const TMultiChunkWriterOptionsPtr Options;
    const NRpc::IChannelPtr MasterChannel;
    const NTransactionClient::TTransactionId TransactionId;
    const TChunkListId ParentChunkListId;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;

    const int UploadReplicationFactor;

    TProviderPtr Provider;

    volatile double Progress;

    //! Total compressed size of data in the completed chunks.
    i64 CompleteChunkSize;

    TAsyncStreamState State;

    TSession CurrentSession;
    TPromise<TSession> NextSession;

    NConcurrency::TParallelAwaiterPtr CloseChunksAwaiter;

    TSpinLock WrittenChunksGuard;
    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunks;

    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_
#include "multi_chunk_sequential_writer-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_

