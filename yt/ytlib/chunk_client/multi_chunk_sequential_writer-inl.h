#ifndef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_sequential_writer.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_

#include "private.h"
#include "chunk_list_ypath_proxy.h"
#include "chunk_ypath_proxy.h"
#include "dispatcher.h"
#include "chunk_ypath_proxy.h"
#include "chunk_helpers.h"
#include "erasure_writer.h"
#include "replication_writer.h"

#include <core/erasure/codec.h>

#include <core/misc/string.h>
#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkWriter>
TMultiChunkSequentialWriter<TChunkWriter>::TMultiChunkSequentialWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        TProviderPtr provider,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TChunkListId& parentChunkListId)
    : Config(config)
    , Options(options)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , ParentChunkListId(parentChunkListId)
    , NodeDirectory(New<NNodeTrackerClient::TNodeDirectory>())
    , UploadReplicationFactor(std::min(Options->ReplicationFactor, Config->UploadReplicationFactor))
    , Provider(provider)
    , Progress(0)
    , CompleteChunkSize(0)
    , CloseChunksAwaiter(New<NConcurrency::TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker()))
    , Logger(ChunkWriterLogger)
{
    YCHECK(config);
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("TransactionId: %s", ~ToString(TransactionId)));
}

template <class TChunkWriter>
TMultiChunkSequentialWriter<TChunkWriter>::~TMultiChunkSequentialWriter()
{ }

template <class TChunkWriter>
TAsyncError TMultiChunkSequentialWriter<TChunkWriter>::AsyncOpen()
{
    YCHECK(!State.HasRunningOperation());

    CreateNextSession();

    State.StartOperation();
    NextSession.Subscribe(BIND(
        &TMultiChunkSequentialWriter::InitCurrentSession,
        MakeWeak(this)));

    return State.GetOperationError();
}

template <class TChunkWriter>
auto TMultiChunkSequentialWriter<TChunkWriter>::GetCurrentWriter() -> TFacade*
{
    if (!CurrentSession.ChunkWriter)
        return nullptr;

    if (CurrentSession.ChunkWriter->GetMetaSize() > Config->MaxMetaSize) {
        LOG_DEBUG("Switching to next chunk: meta is too large (ChunkMetaSize: %" PRId64 ")",
            CurrentSession.ChunkWriter->GetMetaSize());

        SwitchSession();
    } else if (CurrentSession.ChunkWriter->GetCurrentSize() > Config->DesiredChunkSize) {
        i64 currentDataSize = CompleteChunkSize + CurrentSession.ChunkWriter->GetCurrentSize();
        i64 expectedInputSize = static_cast<i64>(currentDataSize * std::max(0.0, 1.0 - Progress));

        if (expectedInputSize > Config->DesiredChunkSize ||
            CurrentSession.ChunkWriter->GetCurrentSize() > 2 * Config->DesiredChunkSize)
        {
            LOG_DEBUG("Switching to next chunk: data is too large (CurrentSessionSize: %" PRId64 ", ExpectedInputSize: %" PRId64 ", DesiredChunkSize: %" PRId64 ")",
                CurrentSession.ChunkWriter->GetCurrentSize(),
                expectedInputSize,
                Config->DesiredChunkSize);

            SwitchSession();
        }
    }

    // If we switched session we should check that new session is ready.
    return CurrentSession.ChunkWriter
        ? CurrentSession.ChunkWriter->GetFacade()
        : nullptr;
}

template <class TChunkWriter>
TAsyncError TMultiChunkSequentialWriter<TChunkWriter>::GetReadyEvent()
{
    if (CurrentSession.ChunkWriter) {
        return CurrentSession.ChunkWriter->GetReadyEvent();
    } else {
        return State.GetOperationError();
    }
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::CreateNextSession()
{
    YCHECK(!NextSession);

    NextSession = NewPromise<TSession>();

    auto chunkType = (Options->ErasureCodec == NErasure::ECodec::None)
        ? NObjectClient::EObjectType::Chunk
        : NObjectClient::EObjectType::ErasureChunk;

    CreateChunk(MasterChannel, Config, Options, chunkType, TransactionId).Subscribe(
            BIND(&TMultiChunkSequentialWriter::OnChunkCreated, MakeWeak(this))
                .Via(TDispatcher::Get()->GetWriterInvoker()));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkCreated(
    NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(NextSession);

    if (!State.IsActive()) {
        return;
    }

    TSession session;

    try {
        NChunkClient::OnChunkCreated(rsp, Config, Options, &session.ChunkId, &session.Replicas, NodeDirectory);
    } catch (const std::exception& ex) {
        State.Fail(ex);
        return;
    }

    LOG_DEBUG("Chunk created (ChunkId: %s)", ~ToString(session.ChunkId));

    auto targets = NodeDirectory->GetDescriptors(session.Replicas);
    auto erasureCodecId = Options->ErasureCodec;
    if (erasureCodecId == NErasure::ECodec::None) {
        session.AsyncWriter = CreateReplicationWriter(
            Config,
            session.ChunkId,
            targets);
    } else {
        auto* erasureCodec = NErasure::GetCodec(erasureCodecId);

        std::vector<IAsyncWriterPtr> writers = CreateErasurePartWriters(
            Config,
            session.ChunkId,
            erasureCodec,
            targets,
            EWriteSessionType::User);

        session.AsyncWriter = CreateErasureWriter(
            Config,
            erasureCodec,
            writers);
    }

    session.AsyncWriter->Open();
    NextSession.Set(session);
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::SetProgress(double progress)
{
    Progress = progress;
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::InitCurrentSession(TSession nextSession)
{
    VERIFY_THREAD_AFFINITY_ANY();

    nextSession.ChunkWriter = Provider->CreateChunkWriter(nextSession.AsyncWriter);
    CurrentSession = nextSession;

    NextSession.Reset();
    CreateNextSession();

    State.FinishOperation();
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::SwitchSession()
{
    State.StartOperation();
    YCHECK(NextSession);

    auto this_ = MakeStrong(this);
    auto startNextSession = [this, this_] (TError error) {
        if (!error.IsOK())
            return;

        NextSession.Subscribe(BIND(
            &TMultiChunkSequentialWriter::InitCurrentSession,
            MakeWeak(this)));
    };

    auto result = FinishCurrentSession();
    if (Config->SyncChunkSwitch) {
        // Wait and block writing, until previous chunks has been completely closed.
        // This prevents double memory accounting in scheduler memory usage estimates.
        result.Subscribe(BIND(startNextSession));
    } else {
        // Start writing next chunk asap.
        startNextSession(TError());
    }

}

template <class TChunkWriter>
TAsyncError TMultiChunkSequentialWriter<TChunkWriter>::FinishCurrentSession()
{
    if (CurrentSession.IsNull()) {
        return MakePromise(TError());
    }

    auto finishResult = NewPromise<TError>();
    if (CurrentSession.ChunkWriter->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~ToString(CurrentSession.ChunkId));

        Provider->OnChunkFinished();

        NChunkClient::NProto::TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), CurrentSession.ChunkId);

        int chunkIndex = -1;
        {
            TGuard<TSpinLock> guard(WrittenChunksGuard);
            chunkIndex = WrittenChunks.size();
            WrittenChunks.push_back(chunkSpec);
        }

        CloseChunksAwaiter->Await(finishResult.ToFuture(), BIND(
            &TMultiChunkSequentialWriter::OnChunkFinished,
            MakeWeak(this),
            CurrentSession.ChunkId));

        CurrentSession.ChunkWriter->AsyncClose().Subscribe(BIND(
            &TMultiChunkSequentialWriter::OnChunkClosed,
            MakeWeak(this),
            chunkIndex,
            CurrentSession,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~ToString(CurrentSession.ChunkId));
        finishResult.Set(TError());
    }

    CurrentSession.Reset();
    return finishResult;
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkClosed(
    int chunkIndex,
    TSession currentSession,
    TAsyncErrorPromise finishResult,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        auto wrappedError = TError("Error closing chunk") << error;
        finishResult.Set(wrappedError);
        return;
    }

    auto asyncWriter = currentSession.AsyncWriter;
    auto chunkWriter = currentSession.ChunkWriter;

    CompleteChunkSize += chunkWriter->GetCurrentSize();

    Provider->OnChunkClosed(chunkWriter);

    LOG_DEBUG("Chunk closed (ChunkId: %s)",
        ~ToString(currentSession.ChunkId));

    std::vector<TChunkReplica> replicas;
    FOREACH (int index, asyncWriter->GetWrittenIndexes()) {
        replicas.push_back(currentSession.Replicas[index]);
    }

    NObjectClient::TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();
    {
        auto req = TChunkYPathProxy::Confirm(
            NCypressClient::FromObjectId(currentSession.ChunkId));
        NMetaState::GenerateMutationId(req);
        *req->mutable_chunk_info() = asyncWriter->GetChunkInfo();
        NYT::ToProto(req->mutable_replicas(), replicas);
        *req->mutable_chunk_meta() = chunkWriter->GetMasterMeta();

        batchReq->AddRequest(req);
    }
    {
        // Initialize the entry earlier prepared in FinishCurrentSession.
        TGuard<TSpinLock> guard(WrittenChunksGuard);
        auto& chunkSpec = WrittenChunks[chunkIndex];
        NYT::ToProto(chunkSpec.mutable_chunk_id(), currentSession.ChunkId);
        NYT::ToProto(chunkSpec.mutable_replicas(), replicas);
        *chunkSpec.mutable_extensions() = chunkWriter->GetSchedulerMeta().extensions();
    }

    batchReq->Invoke().Subscribe(BIND(
        &TMultiChunkSequentialWriter::OnChunkConfirmed,
        MakeWeak(this),
        currentSession.ChunkId,
        finishResult));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkConfirmed(
    TChunkId chunkId,
    TAsyncErrorPromise finishResult,
    NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto error = batchRsp->GetCumulativeError();
    if (!error.IsOK()) {
        auto wrappedError = TError(
            EErrorCode::MasterCommunicationFailed,
            "Error confirming chunk %s",
            ~ToString(chunkId)) << error;

        finishResult.Set(wrappedError);
        return;
    }

    LOG_DEBUG("Chunk confirmed (ChunkId: %s)",
        ~ToString(chunkId));

    finishResult.Set(TError());
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkFinished(
    TChunkId chunkId,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed and registered (ChunkId: %s)",
        ~ToString(chunkId));
}

template <class TChunkWriter>
TAsyncError TMultiChunkSequentialWriter<TChunkWriter>::AsyncClose()
{
    if (State.IsActive()) {
        YCHECK(!State.HasRunningOperation());

        State.StartOperation();
        FinishCurrentSession();

        CloseChunksAwaiter->Complete(BIND(
            &TMultiChunkSequentialWriter::AttachChunks,
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::AttachChunks()
{
    if (!State.IsActive()) {
        return;
    }

    NObjectClient::TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();

    FOREACH (const auto& chunkSpec, WrittenChunks) {
        auto req = TChunkListYPathProxy::Attach(
            NCypressClient::FromObjectId(ParentChunkListId));
        *req->add_children_ids() = chunkSpec.chunk_id();
        NMetaState::GenerateMutationId(req);
        batchReq->AddRequest(req);
    }

    batchReq->Invoke().Subscribe(BIND(
        &TMultiChunkSequentialWriter::OnClose,
        MakeWeak(this)));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnClose(
    NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (!State.IsActive()) {
        return;
    }

    auto error = batchRsp->GetCumulativeError();
    if (!error.IsOK()) {
        auto wrappedError = TError(
            EErrorCode::MasterCommunicationFailed,
            "Error attaching chunks to chunk list %s",
            ~ToString(ParentChunkListId))
            << error;
        State.Fail(wrappedError);
        return;
    }

    LOG_DEBUG("Chunk sequence writer closed");

    State.Close();
    State.FinishOperation();
}

template <class TChunkWriter>
const std::vector<NChunkClient::NProto::TChunkSpec>& TMultiChunkSequentialWriter<TChunkWriter>::GetWrittenChunks() const
{
    return WrittenChunks;
}

template <class TChunkWriter>
NNodeTrackerClient::TNodeDirectoryPtr TMultiChunkSequentialWriter<TChunkWriter>::GetNodeDirectory() const
{
    return NodeDirectory;
}

template <class TChunkWriter>
auto TMultiChunkSequentialWriter<TChunkWriter>::GetProvider() -> TProviderPtr
{
    return Provider;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
