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
#include "helpers.h"
#include "private.h"
#include "replication_writer.h"

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/config.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <core/concurrency/scheduler.h>

#include <core/erasure/codec.h>

#include <core/misc/address.h>
#include <core/misc/finally.h>

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
using namespace NApi;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TNontemplateMultiChunkWriterBase::TNontemplateMultiChunkWriterBase(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    IClientPtr client,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
    : Logger(ChunkClientLogger)
    , Config_(NYTree::CloneYsonSerializable(config))
    , Options_(options)
    , Client_(client)
    , MasterChannel_(client->GetMasterChannel(EMasterChannelKind::Leader))
    , TransactionId_(transactionId)
    , ParentChunkListId_(parentChunkListId)
    , Throttler_(throttler)
    , BlockCache_(blockCache)
    , NodeDirectory_(New<NNodeTrackerClient::TNodeDirectory>())
{
    YCHECK(Config_);
    YCHECK(MasterChannel_);

    Config_->UploadReplicationFactor = std::min(
        Options_->ReplicationFactor,
        Config_->UploadReplicationFactor);

    Logger.AddTag("TransactionId: %v, ChunkListId: %v",
        TransactionId_,
        ParentChunkListId_);
}

TFuture<void> TNontemplateMultiChunkWriterBase::Open()
{
    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::Close()
{
    YCHECK(!Closing_);
    Closing_ = true;

    if (!CompletionError_.IsSet()) {
        FinishSession();

        BIND(&TNontemplateMultiChunkWriterBase::DoClose, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    ReadyEvent_ = CompletionError_.ToFuture();
    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::GetReadyEvent()
{
    if (Session_.IsActive()) {
        return Session_.TemplateWriter->GetReadyEvent();
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
    auto writer = Session_.TemplateWriter;
    if (writer) {
        return DataStatistics_ + writer->GetDataStatistics();
    } else {
        return DataStatistics_;
    }
}

void TNontemplateMultiChunkWriterBase::InitSession()
{
    try {
        auto chunkType = Options_->ErasureCodec == ECodec::None
            ? EObjectType::Chunk
            : EObjectType::ErasureChunk;

        auto rspOrError = WaitFor(CreateChunk(
            MasterChannel_,
            Options_,
            chunkType,
            TransactionId_,
            ParentChunkListId_));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Error creating chunk");

        const auto& rsp = rspOrError.Value();

        Session_.ChunkId = NYT::FromProto<TChunkId>(rsp->object_ids(0));

        LOG_DEBUG("Chunk created (ChunkId: %v)", Session_.ChunkId);

        if (Options_->ErasureCodec == ECodec::None) {
            Session_.UnderlyingWriter = CreateReplicationWriter(
                Config_,
                Options_,
                Session_.ChunkId, 
                TChunkReplicaList(),
                NodeDirectory_,
                Client_,
                BlockCache_,
                Throttler_);
        } else {
            auto* erasureCodec = GetCodec(Options_->ErasureCodec);
            // NB(psushin): we don't ask master for new erasure replicas, 
            // because we cannot guarantee proper replica placement.
            Options_->AllowAllocatingNewTargetNodes = false;
            auto writers = CreateErasurePartWriters(
                Config_,
                Options_,
                Session_.ChunkId, 
                erasureCodec, 
                NodeDirectory_, 
                Client_,
                Throttler_,
                BlockCache_);
            Session_.UnderlyingWriter = CreateErasureWriter(
                Config_,
                Session_.ChunkId,
                erasureCodec,
                writers);
        }

        WaitFor(Session_.UnderlyingWriter->Open())
            .ThrowOnError();

        Session_.TemplateWriter = CreateTemplateWriter(Session_.UnderlyingWriter);

        WaitFor(Session_.TemplateWriter->Open())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        CompletionError_.TrySet(TError("Failed to initialize new writing session")
            << ex);
    }
}

void TNontemplateMultiChunkWriterBase::FinishSession()
{
    ReadyEvent_ = BIND(
        &TNontemplateMultiChunkWriterBase::DoFinishSession,
        MakeStrong(this))
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
    .Run();

    CloseChunkEvents_.push_back(ReadyEvent_);
}

void TNontemplateMultiChunkWriterBase::DoFinishSession()
{
    try {
        TFinallyGuard finally([&] () {
            Session_.Reset();
        });

        if (!Session_.IsActive()) {
            return;
        }

        YCHECK(Session_.TemplateWriter->GetDataSize() > 0);

        // Reserve next sequential slot in #WrittenChunks_.
        WrittenChunks_.push_back(TChunkSpec());
        auto& chunkSpec = WrittenChunks_.back();

        LOG_DEBUG("Finishing chunk (ChunkId: %v)", Session_.ChunkId);

        auto error = WaitFor(Session_.TemplateWriter->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            error, 
            "Failed to close chunk %v",
            Session_.ChunkId);

        LOG_DEBUG("Chunk closed (ChunkId: %v)", Session_.ChunkId);

        auto replicas = Session_.UnderlyingWriter->GetWrittenChunkReplicas();
        YCHECK(!replicas.empty());

        *chunkSpec.mutable_chunk_meta() = Session_.TemplateWriter->GetSchedulerMeta();
        ToProto(chunkSpec.mutable_chunk_id(), Session_.ChunkId);
        NYT::ToProto(chunkSpec.mutable_replicas(), replicas);

        DataStatistics_ += Session_.TemplateWriter->GetDataStatistics();

        auto req = TChunkYPathProxy::Confirm(FromObjectId(Session_.ChunkId));
        GenerateMutationId(req);
        *req->mutable_chunk_info() = Session_.UnderlyingWriter->GetChunkInfo();
        *req->mutable_chunk_meta() = Session_.TemplateWriter->GetMasterMeta();
        NYT::ToProto(req->mutable_replicas(), replicas);

        TObjectServiceProxy objectProxy(MasterChannel_);
        auto rspOrError = WaitFor(objectProxy.Execute(req));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, 
            EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            Session_.ChunkId);

        LOG_DEBUG("Chunk confirmed (ChunkId: %v)", Session_.ChunkId);
    } catch (const std::exception& ex) {
        CompletionError_.TrySet(TError(ex));
    }
}

bool TNontemplateMultiChunkWriterBase::VerifyActive()
{
    YCHECK(!Closing_);

    if (CompletionError_.IsSet()) {
        ReadyEvent_ = CompletionError_.ToFuture();
        return false;
    }

    if (!ReadyEvent_.IsSet()) {
        return false;
    }

    if (!Session_.IsActive()) {
        ReadyEvent_ = BIND(
            &TNontemplateMultiChunkWriterBase::InitSession,
            MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
        return WaitFor(ReadyEvent_).IsOK();
    }

    return true;
}

bool TNontemplateMultiChunkWriterBase::TryFinishSession()
{
    if (Session_.TemplateWriter->GetMetaSize() > Config_->MaxMetaSize) {
        LOG_DEBUG("Switching to next chunk: meta is too large (ChunkMetaSize: %v)",
            Session_.TemplateWriter->GetMetaSize());

        FinishSession();
        return true;
    } 

    if (Session_.TemplateWriter->GetDataSize() > Config_->DesiredChunkSize) {
        i64 currentDataSize = DataStatistics_.compressed_data_size() + Session_.TemplateWriter->GetDataSize();
        i64 expectedInputSize = static_cast<i64>(currentDataSize * std::max(0.0, 1.0 - Progress_));

        if (expectedInputSize > Config_->DesiredChunkSize ||
            // On erasure chunks switch immediately, otherwise we can consume too much memory.
            Options_->ErasureCodec != ECodec::None || 
            Session_.TemplateWriter->GetDataSize() > 2 * Config_->DesiredChunkSize)
        {
            LOG_DEBUG("Switching to next chunk: data is too large (SessionSize: %v, ExpectedInputSize: %v, DesiredChunkSize: %v)",
                Session_.TemplateWriter->GetDataSize(),
                expectedInputSize,
                Config_->DesiredChunkSize);

            FinishSession();
            return true;
        }
    }

    return false;
}

void TNontemplateMultiChunkWriterBase::DoClose()
{
    WaitFor(Combine(CloseChunkEvents_))
        .ThrowOnError();

    LOG_DEBUG("Chunk sequence writer closed (ChunkCount: %v)",
        WrittenChunks_.size());

    CompletionError_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
