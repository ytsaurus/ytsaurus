#include "stdafx.h"
#include "chunk_sequence_writer.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/string.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/object_server/object_service_proxy.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypress;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceWriter::TChunkSequenceWriter(
    TChunkSequenceWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkList,
    const std::vector<TChannel>& channels,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , MasterChannel(masterChannel)
    , Channels(channels)
    , KeyColumns(keyColumns)
    , Progress(0)
    , CompleteChunkSize(0)
    , RowCount(0)
    , TransactionId(transactionId)
    , ParentChunkList(parentChunkList)
    , NextSession(Null)
    , CloseChunksAwaiter(New<TParallelAwaiter>(WriterThread->GetInvoker()))
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(config);
    YASSERT(masterChannel);

    // TODO(babenko): use TTaggedLogger here, tag with parentChunkListId.
}

TChunkSequenceWriter::~TChunkSequenceWriter()
{ }

void TChunkSequenceWriter::CreateNextSession()
{
    YASSERT(NextSession.IsNull());

    NextSession = NewPromise<TSession>();

    LOG_DEBUG("Creating chunk (TransactionId: %s, ReplicationFactor: %d, UploadReplicationFactor: %d)",
        ~TransactionId.ToString(),
        Config->ReplicationFactor,
        Config->UploadReplicationFactor);

    TObjectServiceProxy objectProxy(MasterChannel);
    auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
    req->set_type(EObjectType::Chunk);
    auto* reqExt = req->MutableExtension(TReqCreateChunk::create_chunk);
    reqExt->set_replication_factor(Config->ReplicationFactor);
    reqExt->set_upload_replication_factor(Config->UploadReplicationFactor);
    objectProxy.Execute(req).Subscribe(
        BIND(&TChunkSequenceWriter::OnChunkCreated, MakeWeak(this))
        .Via(WriterThread->GetInvoker()));
}

void TChunkSequenceWriter::OnChunkCreated(TTransactionYPathProxy::TRspCreateObject::TPtr rsp)
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

    auto chunkId = TChunkId::FromProto(rsp->object_id());
    const auto& rspExt = rsp->GetExtension(TRspCreateChunk::create_chunk);
    auto holderAddresses = FromProto<Stroka>(rspExt.holder_addresses());

    if (holderAddresses.size() < Config->UploadReplicationFactor) {
        State.Fail(TError("Not enough holders available"));
        return;
    }

    LOG_DEBUG("Chunk created (Addresses: [%s], ChunkId: %s)",
        ~JoinToString(holderAddresses),
        ~chunkId.ToString());

    TSession session;
    session.RemoteWriter = New<TRemoteWriter>(
        Config->RemoteWriter,
        chunkId,
        holderAddresses);
    session.RemoteWriter->Open();

    session.ChunkWriter = New<TChunkWriter>(
        Config->ChunkWriter,
        session.RemoteWriter,
        Channels,
        KeyColumns);

    // Although we call _Async_Open, it returns immediately.
    // See TChunkWriter for details.
    session.ChunkWriter->AsyncOpen();

    NextSession.Set(session);
}

void TChunkSequenceWriter::SetProgress(double progress)
{
    Progress = progress;
}

TAsyncError TChunkSequenceWriter::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    CreateNextSession();

    State.StartOperation();
    NextSession.Subscribe(BIND(
        &TChunkSequenceWriter::InitCurrentSession,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::InitCurrentSession(TSession nextSession)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // We swap the last key of the previous chunk to the last key of the new chunk.
    if (!CurrentSession.IsNull()) {
        nextSession.ChunkWriter->LastKey.Swap(
            CurrentSession.ChunkWriter->GetLastKey());
    }
    CurrentSession = nextSession;

    NextSession.Reset();
    CreateNextSession();

    State.FinishOperation();
}

TAsyncError TChunkSequenceWriter::AsyncWriteRow(TRow& row, TKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    ++RowCount;

    // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
    auto asyncResult = CurrentSession.ChunkWriter->AsyncWriteRow(row, key);
    if (asyncResult.IsSet()) {
        OnRowWritten(asyncResult.Get());
    } else {
        asyncResult.Subscribe(BIND(&TChunkSequenceWriter::OnRowWritten, MakeWeak(this)));
    }

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnRowWritten(TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (CurrentSession.ChunkWriter->GetCurrentSize() > Config->DesiredChunkSize) {
        auto currentDataSize = CompleteChunkSize + CurrentSession.ChunkWriter->GetCurrentSize();
        auto expectedInputSize = currentDataSize * std::max(.0, 1. - Progress);

        if (expectedInputSize > Config->DesiredChunkSize || 
            CurrentSession.ChunkWriter->GetCurrentSize() > 2 * Config->DesiredChunkSize) 
        {
            LOG_DEBUG("Switching to next chunk (TransactionId: %s, currentSessionSize: %" PRId64 ", ExpectedInputSize: %lf)",
                ~TransactionId.ToString(),
                CurrentSession.ChunkWriter->GetCurrentSize(),
                expectedInputSize);

            YASSERT(!NextSession.IsNull());
            // We're not waiting for chunk to be closed.
            FinishCurrentSession();
            NextSession.Subscribe(BIND(
                &TChunkSequenceWriter::InitCurrentSession,
                MakeWeak(this)));
            return;
        }
    }

    State.FinishOperation(error);
}

void TChunkSequenceWriter::FinishCurrentSession()
{
    if (CurrentSession.IsNull())
        return;

    if (CurrentSession.ChunkWriter->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());

        auto finishResult = NewPromise<TError>();
        CloseChunksAwaiter->Await(finishResult.ToFuture(), BIND(
            &TChunkSequenceWriter::OnChunkFinished, 
            MakeWeak(this),
            CurrentSession.RemoteWriter->GetChunkId()));

        CurrentSession.ChunkWriter->AsyncClose().Subscribe(BIND(
            &TChunkSequenceWriter::OnChunkClosed,
            MakeWeak(this),
            CurrentSession,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentSession.RemoteWriter->GetChunkId().ToString());
    }

    CurrentSession.Reset();
}

void TChunkSequenceWriter::OnChunkClosed(
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

    TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();
    {
        auto req = TChunkYPathProxy::Confirm(FromObjectId(currentSession.RemoteWriter->GetChunkId()));
        *req->mutable_chunk_info() = currentSession.RemoteWriter->GetChunkInfo();
        ToProto(req->mutable_holder_addresses(), currentSession.RemoteWriter->GetHolders());
        *req->mutable_chunk_meta() = currentSession.ChunkWriter->GetMasterMeta();

        batchReq->AddRequest(req);
    }
    {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(ParentChunkList));
        *req->add_children_ids() = currentSession.RemoteWriter->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }
    {
        auto req = TTransactionYPathProxy::ReleaseObject(FromObjectId(TransactionId));
        *req->mutable_object_id() = currentSession.RemoteWriter->GetChunkId().ToProto();

        batchReq->AddRequest(~req);
    }

    batchReq->Invoke().Subscribe(BIND(
        &TChunkSequenceWriter::OnChunkRegistered,
        MakeWeak(this),
        currentSession.RemoteWriter->GetChunkId(),
        finishResult));
}

void TChunkSequenceWriter::OnChunkRegistered(
    TChunkId chunkId,
    TAsyncErrorPromise finishResult,
    TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp)
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

void TChunkSequenceWriter::OnChunkFinished(
    TChunkId chunkId,
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

TAsyncError TChunkSequenceWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    FinishCurrentSession();

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

TKey& TChunkSequenceWriter::GetLastKey()
{
    YASSERT(!State.HasRunningOperation());
    return CurrentSession.ChunkWriter->GetLastKey();
}

const TNullable<TKeyColumns>& TChunkSequenceWriter::GetKeyColumns() const
{
    return KeyColumns;
}

i64 TChunkSequenceWriter::GetRowCount() const
{
    YASSERT(!State.HasRunningOperation());
    return RowCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
