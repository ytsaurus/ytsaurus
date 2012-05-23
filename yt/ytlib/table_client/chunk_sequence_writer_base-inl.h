#ifndef CHUNK_SEQUENCE_WRITER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_sequence_writer_base.h"
#endif
#undef CHUNK_SEQUENCE_WRITER_BASE_INL_H_

#include "private.h"
#include "schema.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/host_name.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkWriter>
TChunkSequenceWriterBase<TChunkWriter>::TChunkSequenceWriterBase(
    TChunkSequenceWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NObjectServer::TTransactionId& transactionId,
    const NChunkServer::TChunkListId& parentChunkList)
    : Config(config)
    , MasterChannel(masterChannel)
    , Progress(0)
    , CompleteChunkSize(0)
    , TransactionId(transactionId)
    , ParentChunkList(parentChunkList)
    , NextSession(Null)
    , CloseChunksAwaiter(New<TParallelAwaiter>(NChunkClient::WriterThread->GetInvoker()))
    , Logger(TableWriterLogger)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(config);
    YASSERT(masterChannel);

    // TODO(babenko): use TTaggedLogger here, tag with parentChunkListId.
}

template <class TChunkWriter>
TChunkSequenceWriterBase<TChunkWriter>::~TChunkSequenceWriterBase()
{ }

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::CreateNextSession()
{
    YASSERT(NextSession.IsNull());

    NextSession = NewPromise<TSession>();

    LOG_DEBUG("Creating chunk (TransactionId: %s, ReplicationFactor: %d, UploadReplicationFactor: %d)",
        ~TransactionId.ToString(),
        Config->ReplicationFactor,
        Config->UploadReplicationFactor);

    NObjectServer::TObjectServiceProxy objectProxy(MasterChannel);

    auto req = NTransactionServer::TTransactionYPathProxy::CreateObject(
        NCypress::FromObjectId(TransactionId));
    req->set_type(NObjectServer::EObjectType::Chunk);

    auto* reqExt = req->MutableExtension(NChunkServer::NProto::TReqCreateChunk::create_chunk);
    reqExt->set_preferred_host_name(Stroka(GetHostName()));
    reqExt->set_replication_factor(Config->ReplicationFactor);
    reqExt->set_upload_replication_factor(Config->UploadReplicationFactor);

    objectProxy.Execute(req).Subscribe(
        BIND(&TChunkSequenceWriterBase::OnChunkCreated, MakeWeak(this))
        .Via(NChunkClient::WriterThread->GetInvoker()));
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnChunkCreated(
    NTransactionServer::TTransactionYPathProxy::TRspCreateObjectPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(!NextSession.IsNull());

    if (!State.IsActive()) {
        return;
    }

    if (!rsp->IsOK()) {
        State.Fail(rsp->GetError());
        return;
    }

    auto chunkId = NChunkServer::TChunkId::FromProto(rsp->object_id());
    const auto& rspExt = rsp->GetExtension(
        NChunkServer::NProto::TRspCreateChunk::create_chunk);
    auto holderAddresses = FromProto<Stroka>(rspExt.node_addresses());

    if (holderAddresses.size() < Config->UploadReplicationFactor) {
        State.Fail(TError("Not enough holders available"));
        return;
    }

    LOG_DEBUG("Chunk created (Addresses: [%s], ChunkId: %s)",
        ~JoinToString(holderAddresses),
        ~chunkId.ToString());

    TSession session;
    session.RemoteWriter = New<NChunkClient::TRemoteWriter>(
        Config->RemoteWriter,
        chunkId,
        holderAddresses);
    session.RemoteWriter->Open();

    PrepareChunkWriter(session);

    NextSession.Set(session);
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::SetProgress(double progress)
{
    Progress = progress;
}

template <class TChunkWriter>
TAsyncError TChunkSequenceWriterBase<TChunkWriter>::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    CreateNextSession();

    State.StartOperation();
    NextSession.Subscribe(BIND(
        &TChunkSequenceWriterBase::InitCurrentSession,
        MakeWeak(this)));

    return State.GetOperationError();
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::InitCurrentSession(TSession nextSession)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CurrentSession = nextSession;

    NextSession.Reset();
    CreateNextSession();

    State.FinishOperation();
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnRowWritten(TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (CurrentSession.ChunkWriter->GetMetaSize() > Config->MaxMetaSize) {
        LOG_DEBUG("Switching to next chunk: too big chunk meta (TransactionId: %s, ChunkMetaSize: %" PRId64,
            ~TransactionId.ToString(),
            CurrentSession.ChunkWriter->GetMetaSize());

        SwitchSession();
        return;
    }


    if (CurrentSession.ChunkWriter->GetCurrentSize() > Config->DesiredChunkSize) 
    {
        auto currentDataSize = CompleteChunkSize + CurrentSession.ChunkWriter->GetCurrentSize();
        auto expectedInputSize = currentDataSize * std::max(.0, 1. - Progress);

        if (expectedInputSize > Config->DesiredChunkSize || 
            CurrentSession.ChunkWriter->GetCurrentSize() > 2 * Config->DesiredChunkSize) 
        {
            LOG_DEBUG("Switching to next chunk (TransactionId: %s, currentSessionSize: %" PRId64 ", ExpectedInputSize: %lf)",
                ~TransactionId.ToString(),
                CurrentSession.ChunkWriter->GetCurrentSize(),
                expectedInputSize);

            SwitchSession();
            return;
        }
    }

    State.FinishOperation(error);
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::SwitchSession()
{
    YASSERT(!NextSession.IsNull());
    // We're not waiting for chunk to be closed.
    FinishCurrentSession();
    NextSession.Subscribe(BIND(
        &TChunkSequenceWriterBase::InitCurrentSession,
        MakeWeak(this)));
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::FinishCurrentSession()
{
    if (CurrentSession.IsNull())
        return;

    if (CurrentSession.ChunkWriter->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());

        auto finishResult = NewPromise<TError>();
        CloseChunksAwaiter->Await(finishResult.ToFuture(), BIND(
            &TChunkSequenceWriterBase::OnChunkFinished, 
            MakeWeak(this),
            CurrentSession.RemoteWriter->GetChunkId()));

        CurrentSession.ChunkWriter->AsyncClose().Subscribe(BIND(
            &TChunkSequenceWriterBase::OnChunkClosed,
            MakeWeak(this),
            CurrentSession,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());
    }

    CurrentSession.Reset();
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnChunkClosed(
    TSession currentSession,
    TAsyncErrorPromise finishResult,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        finishResult.Set(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed (ChunkId: %s)",
        ~currentSession.RemoteWriter->GetChunkId().ToString());

    NObjectServer::TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();
    {
        auto req = NChunkServer::TChunkYPathProxy::Confirm(
            NCypress::FromObjectId(currentSession.RemoteWriter->GetChunkId()));
        *req->mutable_chunk_info() = currentSession.RemoteWriter->GetChunkInfo();
        ToProto(req->mutable_node_addresses(), currentSession.RemoteWriter->GetNodeAddresses());
        *req->mutable_chunk_meta() = currentSession.ChunkWriter->GetMasterMeta();

        batchReq->AddRequest(req);
    }
    {
        auto req = NChunkServer::TChunkListYPathProxy::Attach(
            NCypress::FromObjectId(ParentChunkList));
        *req->add_children_ids() = currentSession.RemoteWriter->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }
    {
        auto req = NTransactionServer::TTransactionYPathProxy::ReleaseObject(
            NCypress::FromObjectId(TransactionId));
        *req->mutable_object_id() = currentSession.RemoteWriter->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }

    {
        NProto::TInputChunk inputChunk;
        auto* slice = inputChunk.mutable_slice();
        slice->mutable_start_limit();
        slice->mutable_end_limit();
        *slice->mutable_chunk_id() = currentSession.RemoteWriter->GetChunkId().ToProto();

        ToProto(inputChunk.mutable_node_addresses(), currentSession.RemoteWriter->GetNodeAddresses());
        *inputChunk.mutable_channel() = TChannel::CreateUniversal().ToProto();
        *inputChunk.mutable_extensions() = currentSession.ChunkWriter->GetMasterMeta().extensions();

        TGuard<TSpinLock> guard(WrittenChunksGuard);
        WrittenChunks.push_back(inputChunk);
    }

    batchReq->Invoke().Subscribe(BIND(
        &TChunkSequenceWriterBase::OnChunkRegistered,
        MakeWeak(this),
        currentSession.RemoteWriter->GetChunkId(),
        finishResult));
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnChunkRegistered(
    NChunkServer::TChunkId chunkId,
    TAsyncErrorPromise finishResult,
    NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!batchRsp->IsOK()) {
        finishResult.Set(batchRsp->GetError());
        return;
    }

    LOG_DEBUG("Chunk registered successfully (ChunkId: %s)",
        ~chunkId.ToString());

    for (int i = 0; i < batchRsp->GetSize(); ++i) {
        auto rsp = batchRsp->GetResponse(i);
        if (!rsp->IsOK()) {
            finishResult.Set(rsp->GetError());
            return;
        }
    }

    finishResult.Set(TError());
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnChunkFinished(
    NChunkServer::TChunkId chunkId,
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
TAsyncError TChunkSequenceWriterBase<TChunkWriter>::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    FinishCurrentSession();

    CloseChunksAwaiter->Complete(BIND(
        &TChunkSequenceWriterBase::OnClose,
        MakeWeak(this)));

    return State.GetOperationError();
}

template <class TChunkWriter>
void TChunkSequenceWriterBase<TChunkWriter>::OnClose()
{
    if (State.IsActive()) {
        State.Close();
    }

    LOG_DEBUG("Sequence writer closed (TransactionId: %s)",
        ~TransactionId.ToString());

    State.FinishOperation();
}

template <class TChunkWriter> 
const std::vector<NProto::TInputChunk>& TChunkSequenceWriterBase<TChunkWriter>::GetWrittenChunks() const
{
    return WrittenChunks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
