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

        TTableChunkSequenceWriter::TTableChunkSequenceWriter(
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

        TTableChunkSequenceWriter::~TTableChunkSequenceWriter()
        { }

        void TTableChunkSequenceWriter::CreateNextSession()
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
                BIND(&TTableChunkSequenceWriter::OnChunkCreated, MakeWeak(this))
                .Via(WriterThread->GetInvoker()));
        }

        void TTableChunkSequenceWriter::OnChunkCreated(TTransactionYPathProxy::TRspCreateObject::TPtr rsp)
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

        void TTableChunkSequenceWriter::SetProgress(double progress)
        {
            Progress = progress;
        }

        TAsyncError TTableChunkSequenceWriter::AsyncOpen()
        {
            YASSERT(!State.HasRunningOperation());

            CreateNextSession();

            State.StartOperation();
            NextSession.Subscribe(BIND(
                &TTableChunkSequenceWriter::InitCurrentSession,
                MakeWeak(this)));

            return State.GetOperationError();
        }

        void TTableChunkSequenceWriter::InitCurrentSession(TSession nextSession)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            // We copy the last key of the previous chunk to the last key of the new chunk.
            if (!CurrentSession.IsNull()) {
                nextSession.ChunkWriter->LastKey = 
                    CurrentSession.ChunkWriter->GetLastKey();
            }
            CurrentSession = nextSession;

            NextSession.Reset();
            CreateNextSession();

            State.FinishOperation();
        }

        TAsyncError TTableChunkSequenceWriter::AsyncWriteRow(TRow& row, const TNonOwningKey& key)
        {
            VERIFY_THREAD_AFFINITY(ClientThread);
            YASSERT(!State.HasRunningOperation());

            State.StartOperation();
            ++RowCount;

            // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
            auto asyncResult = CurrentSession.ChunkWriter->AsyncWriteRow(row, key);
            auto error = asyncResult.TryGet();
            if (error) {
                OnRowWritten(error.Get());
            } else {
                asyncResult.Subscribe(BIND(&TTableChunkSequenceWriter::OnRowWritten, MakeWeak(this)));
            }

            return State.GetOperationError();
        }

        void TTableChunkSequenceWriter::OnRowWritten(TError error)
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

        void TTableChunkSequenceWriter::SwitchSession()
        {
            YASSERT(!NextSession.IsNull());
            // We're not waiting for chunk to be closed.
            FinishCurrentSession();
            NextSession.Subscribe(BIND(
                &TTableChunkSequenceWriter::InitCurrentSession,
                MakeWeak(this)));
        }

        void TTableChunkSequenceWriter::FinishCurrentSession()
        {
            if (CurrentSession.IsNull())
                return;

            if (CurrentSession.ChunkWriter->GetCurrentSize() > 0) {
                LOG_DEBUG("Finishing chunk (ChunkId: %s)",
                    ~CurrentSession.RemoteWriter->GetChunkId().ToString());

                auto finishResult = NewPromise<TError>();
                CloseChunksAwaiter->Await(finishResult.ToFuture(), BIND(
                    &TTableChunkSequenceWriter::OnChunkFinished, 
                    MakeWeak(this),
                    CurrentSession.RemoteWriter->GetChunkId()));

                CurrentSession.ChunkWriter->AsyncClose().Subscribe(BIND(
                    &TTableChunkSequenceWriter::OnChunkClosed,
                    MakeWeak(this),
                    CurrentSession,
                    finishResult));

            } else {
                LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
                    ~CurrentSession.RemoteWriter->GetChunkId().ToString());
            }

            CurrentSession.Reset();
        }

        void TTableChunkSequenceWriter::OnChunkClosed(
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

            {
                NProto::TInputChunk inputChunk;
                auto* slice = inputChunk.mutable_slice();
                slice->mutable_start_limit();
                slice->mutable_end_limit();
                *slice->mutable_chunk_id() = currentSession.RemoteWriter->GetChunkId().ToProto();

                ToProto(inputChunk.mutable_holder_addresses(), currentSession.RemoteWriter->GetHolders());
                *inputChunk.mutable_channel() = TChannel::CreateUniversal().ToProto();
                *inputChunk.mutable_extensions() = 
                    currentSession.ChunkWriter->GetMasterMeta().extensions();

                TGuard<TSpinLock> guard(WrittenChunksGuard);
                WrittenChunks.push_back(inputChunk);
            }

            batchReq->Invoke().Subscribe(BIND(
                &TTableChunkSequenceWriter::OnChunkRegistered,
                MakeWeak(this),
                currentSession.RemoteWriter->GetChunkId(),
                finishResult));
        }

        void TTableChunkSequenceWriter::OnChunkRegistered(
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

        void TTableChunkSequenceWriter::OnChunkFinished(
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

        TAsyncError TTableChunkSequenceWriter::AsyncClose()
        {
            VERIFY_THREAD_AFFINITY(ClientThread);
            YASSERT(!State.HasRunningOperation());

            State.StartOperation();
            FinishCurrentSession();

            CloseChunksAwaiter->Complete(BIND(
                &TTableChunkSequenceWriter::OnClose,
                MakeWeak(this)));

            return State.GetOperationError();
        }

        void TTableChunkSequenceWriter::OnClose()
        {
            if (State.IsActive()) {
                State.Close();
            }

            LOG_DEBUG("Sequence writer closed (TransactionId: %s)",
                ~TransactionId.ToString());

            State.FinishOperation();
        }

        const TOwningKey& TTableChunkSequenceWriter::GetLastKey() const
        {
            YASSERT(!State.HasRunningOperation());
            return CurrentSession.ChunkWriter->GetLastKey();
        }

        const TNullable<TKeyColumns>& TTableChunkSequenceWriter::GetKeyColumns() const
        {
            return KeyColumns;
        }

        i64 TTableChunkSequenceWriter::GetRowCount() const
        {
            YASSERT(!State.HasRunningOperation());
            return RowCount;
        }

        const std::vector<NProto::TInputChunk>& TTableChunkSequenceWriter::GetWrittenChunks() const
        {
            return WrittenChunks;
        }

        ////////////////////////////////////////////////////////////////////////////////

    } // namespace NYT 
} // namespace NTableClient
