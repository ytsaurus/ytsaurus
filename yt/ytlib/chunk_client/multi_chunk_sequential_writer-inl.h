#ifndef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_sequential_writer.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_

#include "private.h"
#include "chunk_list_ypath_proxy.h"
#include "chunk_ypath_proxy.h"
#include "dispatcher.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/address.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

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
    , UploadReplicationFactor(std::min(Options->ReplicationFactor, Config->UploadReplicationFactor))
    , Provider(provider)
    , Progress(0)
    , CompleteChunkSize(0)
    , CloseChunksAwaiter(New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker()))
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
            LOG_DEBUG(
                "Switching to next chunk: data is too large (CurrentSessionSize: %" PRId64 ", ExpectedInputSize: %" PRId64 ", DesiredChunkSize: %" PRId64 ")",
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
    if (State.HasRunningOperation()) {
        return State.GetOperationError();
    }

    YCHECK(CurrentSession.ChunkWriter);
    return CurrentSession.ChunkWriter->GetReadyEvent();
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::CreateNextSession()
{
    YCHECK(!NextSession);

    NextSession = NewPromise<TSession>();

    LOG_DEBUG("Creating chunk (ReplicationFactor: %d, UploadReplicationFactor: %d)",
        Options->ReplicationFactor,
        UploadReplicationFactor);

    NObjectClient::TObjectServiceProxy objectProxy(MasterChannel);

    auto req = NObjectClient::TMasterYPathProxy::CreateObject();
    *req->mutable_transaction_id() = TransactionId.ToProto();
    NMetaState::GenerateRpcMutationId(req);
    req->set_type(NObjectClient::EObjectType::Chunk);
    req->set_account(Options->Account);

    auto* reqExt = req->MutableExtension(NChunkClient::NProto::TReqCreateChunkExt::create_chunk);
    if (Config->PreferLocalHost) {
        reqExt->set_preferred_host_name(TAddressResolver::Get()->GetLocalHostName());
    }
    reqExt->set_replication_factor(Options->ReplicationFactor);
    reqExt->set_upload_replication_factor(UploadReplicationFactor);
    reqExt->set_movable(Config->ChunksMovable);
    reqExt->set_vital(Config->ChunksVital);

    objectProxy.Execute(req).Subscribe(
        BIND(&TMultiChunkSequentialWriter::OnChunkCreated, MakeWeak(this))
            .Via(NChunkClient::TDispatcher::Get()->GetWriterInvoker()));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkCreated(
    NObjectClient::TMasterYPathProxy::TRspCreateObjectPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(NextSession);

    if (!State.IsActive()) {
        return;
    }

    if (!rsp->IsOK()) {
        auto wrappedError = TError(
            EErrorCode::MasterCommunicationFailed,
            "Error creating chunk") << *rsp;
        State.Fail(wrappedError);
        return;
    }

    auto chunkId = TChunkId::FromProto(rsp->object_id());
    const auto& rspExt = rsp->GetExtension(NChunkClient::NProto::TRspCreateChunkExt::create_chunk);
    auto addresses = FromProto<Stroka>(rspExt.node_addresses());
    if (addresses.size() < UploadReplicationFactor) {
        State.Fail(TError("Not enough data nodes available: %d received, %d needed",
            static_cast<int>(addresses.size()),
            UploadReplicationFactor));
        return;
    }

    LOG_DEBUG("Chunk created (Addresses: [%s], ChunkId: %s)",
        ~JoinToString(addresses),
        ~chunkId.ToString());

    TSession session;
    session.RemoteWriter = New<NChunkClient::TRemoteWriter>(
        Config,
        chunkId,
        addresses);
    session.RemoteWriter->Open();

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

    nextSession.ChunkWriter = Provider->CreateChunkWriter(nextSession.RemoteWriter);
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
    // We're not waiting for the chunk to close.
    FinishCurrentSession();
    NextSession.Subscribe(BIND(
        &TMultiChunkSequentialWriter::InitCurrentSession,
        MakeWeak(this)));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::FinishCurrentSession()
{
    if (CurrentSession.IsNull())
        return;

    if (CurrentSession.ChunkWriter->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());

        Provider->OnChunkFinished();

        NTableClient::NProto::TInputChunk inputChunk;
        *inputChunk.mutable_chunk_id() = CurrentSession.RemoteWriter->GetChunkId().ToProto();

        TGuard<TSpinLock> guard(WrittenChunksGuard);
        int chunkIndex = WrittenChunks.size();
        WrittenChunks.push_back(inputChunk);

        auto finishResult = NewPromise<TError>();
        CloseChunksAwaiter->Await(finishResult.ToFuture(), BIND(
            &TMultiChunkSequentialWriter::OnChunkFinished,
            MakeWeak(this),
            CurrentSession.RemoteWriter->GetChunkId()));

        CurrentSession.ChunkWriter->AsyncClose().Subscribe(BIND(
            &TMultiChunkSequentialWriter::OnChunkClosed,
            MakeWeak(this),
            chunkIndex,
            CurrentSession,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());
    }

    CurrentSession.Reset();
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

    auto remoteWriter = currentSession.RemoteWriter;
    auto chunkWriter = currentSession.ChunkWriter;

    CompleteChunkSize += chunkWriter->GetCurrentSize();

    LOG_DEBUG("Chunk closed (ChunkId: %s)",
        ~remoteWriter->GetChunkId().ToString());

    NObjectClient::TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();
    {
        auto req = NChunkClient::TChunkYPathProxy::Confirm(
            NCypressClient::FromObjectId(remoteWriter->GetChunkId()));
        NMetaState::GenerateRpcMutationId(req);
        *req->mutable_chunk_info() = remoteWriter->GetChunkInfo();
        ToProto(req->mutable_node_addresses(), remoteWriter->GetNodeAddresses());
        *req->mutable_chunk_meta() = chunkWriter->GetMasterMeta();

        batchReq->AddRequest(req);
    }
    {
        TGuard<TSpinLock> guard(WrittenChunksGuard);
        auto& inputChunk = WrittenChunks[chunkIndex];

        ToProto(inputChunk.mutable_node_addresses(), remoteWriter->GetNodeAddresses());
        *inputChunk.mutable_extensions() = chunkWriter->GetSchedulerMeta().extensions();
    }

    batchReq->Invoke().Subscribe(BIND(
        &TMultiChunkSequentialWriter::OnChunkConfirmed,
        MakeWeak(this),
        remoteWriter->GetChunkId(),
        finishResult));
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkConfirmed(
    NChunkClient::TChunkId chunkId,
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
        ~chunkId.ToString());

    finishResult.Set(TError());
}

template <class TChunkWriter>
void TMultiChunkSequentialWriter<TChunkWriter>::OnChunkFinished(
    NChunkClient::TChunkId chunkId,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed and registered (ChunkId: %s)",
        ~chunkId.ToString());
}

template <class TChunkWriter>
TAsyncError TMultiChunkSequentialWriter<TChunkWriter>::AsyncClose()
{
    YCHECK(!State.HasRunningOperation());

    State.StartOperation();
    FinishCurrentSession();

    CloseChunksAwaiter->Complete(BIND(
        &TMultiChunkSequentialWriter::AttachChunks,
        MakeWeak(this)));

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

    FOREACH (const auto& inputChunk, WrittenChunks) {
        auto req = NChunkClient::TChunkListYPathProxy::Attach(
            NCypressClient::FromObjectId(ParentChunkListId));
        *req->add_children_ids() = inputChunk.chunk_id();
        NMetaState::GenerateRpcMutationId(req);
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
const std::vector<NTableClient::NProto::TInputChunk>& TMultiChunkSequentialWriter<TChunkWriter>::GetWrittenChunks() const
{
    return WrittenChunks;
}

template <class TChunkWriter>
auto TMultiChunkSequentialWriter<TChunkWriter>::GetProvider() -> TProviderPtr
{
    return Provider;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
