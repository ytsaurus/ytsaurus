#include "stdafx.h"

#include "multi_chunk_writer_base.h"

#include "chunk_writer.h"
#include "chunk_list_ypath_proxy.h"
#include "chunk_replica.h"
#include "chunk_writer_base.h"
#include "chunk_ypath_proxy.h"
#include "config.h"
#include "dispatcher.h"
#include "erasure_writer.h"
#include "private.h"
#include "replication_writer.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/erasure/codec.h>

#include <core/misc/address.h>

#include <core/rpc/channel.h>
#include <core/rpc/helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NErasure;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TNontemplateMultiChunkWriterBase::TNontemplateMultiChunkWriterBase(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId)
    : Logger(ChunkClientLogger)
    , Config_(config)
    , Options_(options)
    , MasterChannel_(masterChannel)
    , TransactionId_(transactionId)
    , ParentChunkListId_(parentChunkListId)
    , NodeDirectory_(New<TNodeDirectory>())
    , UploadReplicationFactor_(std::min(Options_->ReplicationFactor, Config_->UploadReplicationFactor))
    , Progress_(0)
    , Closing_(false)
    , ReadyEvent_(VoidFuture)
    , CompletionError_(NewPromise<void>())
    , CloseChunksAwaiter_(New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker()))
{
    YCHECK(Config_);
    YCHECK(MasterChannel_);

    Logger.AddTag("TransactionId: %v", TransactionId_);
}

TFuture<void> TNontemplateMultiChunkWriterBase::Open()
{
    ReadyEvent_= BIND(&TNontemplateMultiChunkWriterBase::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();

    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::Close()
{
    YCHECK(!Closing_);
    Closing_ = true;

    if (CompletionError_.IsSet()) {
        return CompletionError_.ToFuture();
    }

    FinishSession(CurrentSession_);
    CurrentSession_.Reset();

    BIND(&TNontemplateMultiChunkWriterBase::DoClose, MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();

    ReadyEvent_ = CompletionError_.ToFuture();
    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::GetReadyEvent()
{
    if (CurrentSession_.IsActive()) {
        return CurrentSession_.TemplateWriter->GetReadyEvent();
    } else {
        return ReadyEvent_;
    }
}

void TNontemplateMultiChunkWriterBase::SetProgress(double progress)
{
    Progress_ = progress;
}

const std::vector<TChunkSpec>& TNontemplateMultiChunkWriterBase::GetWrittenChunks() const
{
    return WrittenChunks_;
}

TNodeDirectoryPtr TNontemplateMultiChunkWriterBase::GetNodeDirectory() const
{
    return NodeDirectory_;
}

TDataStatistics TNontemplateMultiChunkWriterBase::GetDataStatistics() const
{
    auto writer = CurrentSession_.TemplateWriter;
    if (writer) {
        return DataStatistics_ + writer->GetDataStatistics();
    } else {
        return DataStatistics_;
    }
}

void TNontemplateMultiChunkWriterBase::DoOpen()
{
    CreateNextSession();
    NextSessionReady_ = VoidFuture;
    InitCurrentSession();
}

void TNontemplateMultiChunkWriterBase::CreateNextSession()
{
    LOG_DEBUG("Creating chunk (ReplicationFactor: %v, UploadReplicationFactor: %v)",
        Options_->ReplicationFactor,
        UploadReplicationFactor_);

    try {
        TObjectServiceProxy objectProxy(MasterChannel_);

        auto req = TMasterYPathProxy::CreateObjects();
        ToProto(req->mutable_transaction_id(), TransactionId_);

        auto type = Options_->ErasureCodec == ECodec::None
            ? EObjectType::Chunk
            : EObjectType::ErasureChunk;
        req->set_type(static_cast<int>(type));

        req->set_account(Options_->Account);
        GenerateMutationId(req);

        // ToDo(psushin): Use CreateChunk here.

        auto* reqExt = req->MutableExtension(NProto::TReqCreateChunkExt::create_chunk_ext);
        reqExt->set_movable(Config_->ChunksMovable);
        reqExt->set_replication_factor(Options_->ReplicationFactor);
        reqExt->set_vital(Options_->ChunksVital);
        reqExt->set_erasure_codec(static_cast<int>(Options_->ErasureCodec));

        auto rspOrError = WaitFor(objectProxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Error creating chunk");
        const auto& rsp = rspOrError.Value();

        NextSession_.ChunkId = NYT::FromProto<TChunkId>(rsp->object_ids(0));

        LOG_DEBUG("Chunk created (ChunkId: %v)", NextSession_.ChunkId);

        if (Options_->ErasureCodec == ECodec::None) {
            NextSession_.UnderlyingWriter = CreateReplicationWriter(
                Config_, 
                NextSession_.ChunkId, 
                TChunkReplicaList(),
                NodeDirectory_,
                MasterChannel_);
        } else {
            auto* erasureCodec = GetCodec(Options_->ErasureCodec);
            auto writers = CreateErasurePartWriters(
                Config_, 
                NextSession_.ChunkId, 
                erasureCodec, 
                NodeDirectory_, 
                MasterChannel_, 
                EWriteSessionType::User);

            NextSession_.UnderlyingWriter = CreateErasureWriter(Config_, erasureCodec, writers);
        }

        WaitFor(NextSession_.UnderlyingWriter->Open())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to start new session") << ex;
        LOG_WARNING(error);
        CompletionError_.TrySet(error);
    }
}

void TNontemplateMultiChunkWriterBase::SwitchSession()
{
    auto currentSession = CurrentSession_;
    CurrentSession_.Reset();
    
    ReadyEvent_ = BIND(
        &TNontemplateMultiChunkWriterBase::DoSwitchSession,
        MakeStrong(this),
        currentSession)
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
    .Run();
}

void TNontemplateMultiChunkWriterBase::DoSwitchSession(const TSession& session)
{
    if (Config_->SyncChunkSwitch) {
        // Wait until session is finished.
        WaitFor(FinishSession(session))
            .ThrowOnError();
    } else {
        // Do not wait, fire and move on.
        FinishSession(session);
    }

    InitCurrentSession();
}

TFuture<void> TNontemplateMultiChunkWriterBase::FinishSession(const TSession& session)
{
    auto sessionFinishedEvent = BIND(
        &TNontemplateMultiChunkWriterBase::DoFinishSession,
        MakeWeak(this), 
        session)
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
    .Run();

    CloseChunksAwaiter_->Await(sessionFinishedEvent);

    return sessionFinishedEvent;
}

void TNontemplateMultiChunkWriterBase::DoFinishSession(const TSession& session)
{
    if (session.TemplateWriter->GetDataSize() == 0) {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %v)", session.ChunkId);
        return;
    }

    // Reserve next sequential slot in WrittenChunks_.
    WrittenChunks_.push_back(TChunkSpec());
    auto& chunkSpec = WrittenChunks_.back();

    LOG_DEBUG("Finishing chunk (ChunkId: %v)", session.ChunkId);

    auto error = WaitFor(session.TemplateWriter->Close());

    if (!error.IsOK()) {
        CompletionError_.TrySet(TError(
            "Failed to close chunk %v",
            session.ChunkId)
            << error);
        return;
    }

    LOG_DEBUG("Chunk closed (ChunkId: %v)", session.ChunkId);

    auto replicas = session.UnderlyingWriter->GetWrittenChunkReplicas();

    *chunkSpec.mutable_chunk_meta() = session.TemplateWriter->GetSchedulerMeta();
    ToProto(chunkSpec.mutable_chunk_id(), session.ChunkId);
    NYT::ToProto(chunkSpec.mutable_replicas(), replicas);

    DataStatistics_ += session.TemplateWriter->GetDataStatistics();

    auto req = TChunkYPathProxy::Confirm(FromObjectId(session.ChunkId));
    GenerateMutationId(req);
    *req->mutable_chunk_info() = session.UnderlyingWriter->GetChunkInfo();
    *req->mutable_chunk_meta() = session.TemplateWriter->GetMasterMeta();
    NYT::ToProto(req->mutable_replicas(), replicas);

    TObjectServiceProxy objectProxy(MasterChannel_);
    auto rspOrError = WaitFor(objectProxy.Execute(req));

    if (!rspOrError.IsOK()) {
        CompletionError_.TrySet(TError(
            "Failed to confirm chunk %v",
            session.ChunkId)
            << rspOrError);
        return;
    }

    LOG_DEBUG("Chunk confirmed (ChunkId: %v)", session.ChunkId);
}

void TNontemplateMultiChunkWriterBase::InitCurrentSession()
{
    WaitFor(NextSessionReady_)
        .ThrowOnError();

    auto maybeError = CompletionError_.TryGet();
    if (maybeError) {
        maybeError->ThrowOnError();
    }

    CurrentSession_ = NextSession_;
    NextSession_.Reset();

    CurrentSession_.TemplateWriter = CreateTemplateWriter(CurrentSession_.UnderlyingWriter);

    NextSessionReady_ = BIND(&TNontemplateMultiChunkWriterBase::CreateNextSession, MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();

    WaitFor(CurrentSession_.TemplateWriter->Open());
}

bool TNontemplateMultiChunkWriterBase::VerifyActive()
{
    YCHECK(!Closing_);
    YCHECK(CurrentSession_.IsActive());

    if (CompletionError_.IsSet()) {
        ReadyEvent_ = CompletionError_.ToFuture();
        return false;
    }

    return true;
}

bool TNontemplateMultiChunkWriterBase::TrySwitchSession()
{
    if (CurrentSession_.TemplateWriter->GetMetaSize() > Config_->MaxMetaSize) {
        LOG_DEBUG("Switching to next chunk: meta is too large (ChunkMetaSize: %v)",
            CurrentSession_.TemplateWriter->GetMetaSize());

        SwitchSession();
        return true;
    } 

    if (CurrentSession_.TemplateWriter->GetDataSize() > Config_->DesiredChunkSize) {
        i64 currentDataSize = DataStatistics_.compressed_data_size() + CurrentSession_.TemplateWriter->GetDataSize();
        i64 expectedInputSize = static_cast<i64>(currentDataSize * std::max(0.0, 1.0 - Progress_));

        if (expectedInputSize > Config_->DesiredChunkSize ||
            CurrentSession_.TemplateWriter->GetDataSize() > 2 * Config_->DesiredChunkSize)
        {
            LOG_DEBUG("Switching to next chunk: data is too large (CurrentSessionSize: %v, ExpectedInputSize: %" PRId64 ", DesiredChunkSize: %" PRId64 ")",
                CurrentSession_.TemplateWriter->GetDataSize(),
                expectedInputSize,
                Config_->DesiredChunkSize);

            SwitchSession();
            return true;
        }
    }

    return false;
}

void TNontemplateMultiChunkWriterBase::DoClose()
{
    WaitFor(CloseChunksAwaiter_->Complete())
        .ThrowOnError();

    if (CompletionError_.IsSet()) {
        return;
    }

    if (ParentChunkListId_ == NullChunkListId) {
        LOG_DEBUG("Chunk sequence writer closed, no chunks attached");
        CompletionError_.TrySet(TError());
        return;
    }


    LOG_DEBUG("Attaching %v chunks", WrittenChunks_.size());

    auto req = TChunkListYPathProxy::Attach(FromObjectId(ParentChunkListId_));
    GenerateMutationId(req);
    for (const auto& chunkSpec : WrittenChunks_) {
        *req->add_children_ids() = chunkSpec.chunk_id();
    }

    TObjectServiceProxy objectProxy(MasterChannel_);
    auto rspOrError = WaitFor(objectProxy.Execute(req));

    if (!rspOrError.IsOK()) {
        CompletionError_.TrySet(TError(
            EErrorCode::MasterCommunicationFailed, 
            "Error attaching chunks to chunk list %v",
            ParentChunkListId_)
            << rspOrError);
        return;
    }

    LOG_DEBUG("Chunks attached, chunk sequence writer closed");
    CompletionError_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
