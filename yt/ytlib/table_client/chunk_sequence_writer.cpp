#include "stdafx.h"
#include "chunk_sequence_writer.h"

#include <ytlib/chunk_client/writer_thread.h>
#include <ytlib/misc/string.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypress;
using namespace NTransactionServer;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceWriter::TChunkSequenceWriter(
    TConfig::TPtr config,
    NRpc::IChannel::TPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkList,
    i64 expectedRowCount)
    : Config(config)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , ParentChunkList(parentChunkList)
    , CloseChunksAwaiter(New<TParallelAwaiter>(WriterThread->GetInvoker()))
    , ExpectedRowCount(expectedRowCount)
    , CurrentRowCount(0)
    , CompleteChunkSize(0)
    , NextChunk(Null)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(config);
    YASSERT(masterChannel);

    // TODO(babenko): use TTaggedLogger here
}

TChunkSequenceWriter::~TChunkSequenceWriter()
{ }

void TChunkSequenceWriter::CreateNextChunk()
{
    YASSERT(NextChunk.IsNull());

    NextChunk = NewPromise<TChunkWriter::TPtr>();

    LOG_DEBUG("Creating chunk (TransactionId: %s; UploadReplicaCount: %d)",
        ~TransactionId.ToString(),
        Config->UploadReplicaCount);

    TCypressServiceProxy cypressProxy(MasterChannel);
    auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
    req->set_type(EObjectType::Chunk);
    auto* reqExt = req->MutableExtension(TReqCreateChunk::create_chunk);
    reqExt->set_holder_count(Config->UploadReplicaCount);
    cypressProxy.Execute(req).Subscribe(
        BIND(&TChunkSequenceWriter::OnChunkCreated, MakeWeak(this))
        .Via(WriterThread->GetInvoker()));
}

void TChunkSequenceWriter::OnChunkCreated(TTransactionYPathProxy::TRspCreateObject::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(!NextChunk.IsNull());

    if (!State.IsActive()) {
        return;
    }

    if (!rsp->IsOK()) {
        State.Fail(rsp->GetError());
        return;
    }

    auto chunkId = TChunkId::FromProto(rsp->object_id());
    const auto& rspExt = rsp->GetExtension(TRspCreateChunk::create_chunk);
    auto addresses = FromProto<Stroka>(rspExt.holder_addresses());

    LOG_DEBUG("Chunk created (Addresses: [%s]; ChunkId: %s)",
        ~JoinToString(addresses),
        ~chunkId.ToString());

    auto remoteWriter = New<TRemoteWriter>(
        ~Config->RemoteWriter,
        chunkId,
        addresses);
    remoteWriter->Open();

    auto chunkWriter = New<TChunkWriter>(
        ~Config->ChunkWriter,
        ~remoteWriter);

    // Although we call _Async_Open, it returns immediately.
    // See TChunkWriter for details.
    chunkWriter->AsyncOpen(Attributes);

    NextChunk.Set(chunkWriter);
}

TAsyncError TChunkSequenceWriter::AsyncOpen(
    const NProto::TTableChunkAttributes& attributes)
{
    YASSERT(!State.HasRunningOperation());

    Attributes = attributes;
    CreateNextChunk();

    State.StartOperation();
    NextChunk.Subscribe(BIND(
        &TChunkSequenceWriter::InitCurrentChunk,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::InitCurrentChunk(TChunkWriter::TPtr nextChunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CurrentChunk = nextChunk;
    NextChunk.Reset();
    CreateNextChunk();

    State.FinishOperation();
}

TAsyncError TChunkSequenceWriter::AsyncEndRow(
    const TKey& key,
    const std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();

    ++CurrentRowCount;

    CurrentChunk->AsyncEndRow(key, channels).Subscribe(BIND(
        &TChunkSequenceWriter::OnRowEnded,
        MakeWeak(this),
        channels));

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnRowEnded(
    const std::vector<TChannelWriter::TPtr>& channels,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (CurrentChunk->GetCurrentSize() > Config->DesiredChunkSize) {
        auto expectedInputSize = 
            (CompleteChunkSize + CurrentChunk->GetCurrentSize())
            / static_cast<double>(CurrentRowCount) 
            * (ExpectedRowCount - CurrentRowCount);

        if (expectedInputSize > Config->DesiredChunkSize) {
            LOG_DEBUG(
                "Switching to next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ", ExpectedInputSize %f)",
                ~TransactionId.ToString(),
                CurrentChunk->GetCurrentSize(),
                expectedInputSize);

            YASSERT(!NextChunk.IsNull());
            // We're not waiting for chunk to be closed.
            FinishCurrentChunk(channels);
            NextChunk.Subscribe(BIND(
                &TChunkSequenceWriter::InitCurrentChunk,
                MakeWeak(this)));
            return;
        }
    }

    State.FinishOperation(error);
}

void TChunkSequenceWriter::FinishCurrentChunk(
    const std::vector<TChannelWriter::TPtr>& channels)
{
    if (!CurrentChunk)
        return;

    if (CurrentChunk->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());

        auto finishResult = NewPromise<TError>();
        CloseChunksAwaiter->Await(finishResult.ToFuture(),
            BIND(
                &TChunkSequenceWriter::OnChunkFinished, 
                MakeWeak(this),
                CurrentChunk->GetChunkId()));

        CurrentChunk->AsyncClose(channels).Subscribe(BIND(
            &TChunkSequenceWriter::OnChunkClosed,
            MakeWeak(this),
            CurrentChunk,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());
    }

    CurrentChunk.Reset();
}

void TChunkSequenceWriter::OnChunkClosed(
    TChunkWriter::TPtr currentChunk,
    TAsyncErrorPromise finishResult,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        finishResult.Set(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed (ChunkId: %s).",
        ~currentChunk->GetChunkId().ToString());

    TCypressServiceProxy cypressProxy(MasterChannel);
    auto batchReq = cypressProxy.ExecuteBatch();
    batchReq->AddRequest(~currentChunk->GetConfirmRequest());
    {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(ParentChunkList));
        *req->add_children_ids() = currentChunk->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }
    {
        auto req = TTransactionYPathProxy::ReleaseObject(FromObjectId(TransactionId));
        *req->mutable_object_id() = currentChunk->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }

    batchReq->Invoke().Subscribe(BIND(
        &TChunkSequenceWriter::OnChunkRegistered,
        MakeWeak(this),
        currentChunk->GetChunkId(),
        finishResult));
}

void TChunkSequenceWriter::OnChunkRegistered(
    TChunkId chunkId,
    TAsyncErrorPromise finishResult,
    TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!batchRsp->IsOK()) {
        finishResult.Set(batchRsp->GetError());
        return;
    }

    LOG_DEBUG("Batch chunk registration request succeeded (ChunkId: %s).",
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

void TChunkSequenceWriter::OnChunkFinished(
    TChunkId chunkId,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed and registered (ChunkId: %s).",
        ~chunkId.ToString());
}

TAsyncError TChunkSequenceWriter::AsyncClose(
    const std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();
    FinishCurrentChunk(channels);

    CloseChunksAwaiter->Complete(BIND(
        &TChunkSequenceWriter::OnClose,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnClose()
{
    if (State.IsActive()) {
        State.Close();
    }

    LOG_DEBUG("Sequence writer closed (TransactionId: %s)",
        ~TransactionId.ToString());

    State.FinishOperation();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
