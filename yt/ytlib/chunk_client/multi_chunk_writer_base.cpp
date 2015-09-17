#include "stdafx.h"

#include "multi_chunk_writer_base.h"

#include "chunk_writer.h"
#include "chunk_replica.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "dispatcher.h"
#include "private.h"
#include "lazy_chunk_writer.h"

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/config.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/address.h>

#include <core/rpc/channel.h>
#include <core/rpc/helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NErasure;
using namespace NNodeTrackerClient;
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

    Logger.AddTag("TransactionId: %v", TransactionId_);
}

TFuture<void> TNontemplateMultiChunkWriterBase::Open()
{
    ReadyEvent_= BIND(&TNontemplateMultiChunkWriterBase::InitSession, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();

    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::Close()
{
    YCHECK(!Closing_);
    YCHECK(ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK());

    Closing_ = true;
    ReadyEvent_ = BIND(&TNontemplateMultiChunkWriterBase::FinishSession, MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();

    return ReadyEvent_;
}

TFuture<void> TNontemplateMultiChunkWriterBase::GetReadyEvent()
{
    if (SwitchingSession_) {
        return ReadyEvent_;
    } else {
        return CurrentSession_.TemplateWriter->GetReadyEvent();
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
    TGuard<TSpinLock> guard(SpinLock_);
    if (CurrentSession_.IsActive()) {
        return DataStatistics_ + CurrentSession_.TemplateWriter->GetDataStatistics();
    } else {
        return DataStatistics_;
    }
}

void TNontemplateMultiChunkWriterBase::SwitchSession()
{
    SwitchingSession_ = true;
    ReadyEvent_ = BIND(
        &TNontemplateMultiChunkWriterBase::DoSwitchSession,
        MakeWeak(this))
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
    .Run();
}

void TNontemplateMultiChunkWriterBase::DoSwitchSession()
{
    FinishSession();
    InitSession();
}

void TNontemplateMultiChunkWriterBase::FinishSession()
{
    if (CurrentSession_.TemplateWriter->GetDataSize() == 0) {
        return;
    }

    WaitFor(CurrentSession_.TemplateWriter->Close())
        .ThrowOnError();

    TChunkSpec chunkSpec;
    *chunkSpec.mutable_chunk_meta() = CurrentSession_.TemplateWriter->GetSchedulerMeta();
    ToProto(chunkSpec.mutable_chunk_id(), CurrentSession_.UnderlyingWriter->GetChunkId());
    NYT::ToProto(chunkSpec.mutable_replicas(), CurrentSession_.UnderlyingWriter->GetWrittenChunkReplicas());

    WrittenChunks_.push_back(chunkSpec);

    TGuard<TSpinLock> guard(SpinLock_);
    DataStatistics_ += CurrentSession_.TemplateWriter->GetDataStatistics();
    CurrentSession_.Reset();
}

void TNontemplateMultiChunkWriterBase::InitSession()
{
    CurrentSession_.UnderlyingWriter = CreateLazyChunkWriter(
        Config_,
        Options_,
        TransactionId_,
        ParentChunkListId_,
        NodeDirectory_,
        Client_,
        BlockCache_,
        Throttler_);

    WaitFor(CurrentSession_.UnderlyingWriter->Open())
        .ThrowOnError();

    CurrentSession_.TemplateWriter = CreateTemplateWriter(CurrentSession_.UnderlyingWriter);
    WaitFor(CurrentSession_.TemplateWriter->Open())
        .ThrowOnError();

    SwitchingSession_ = false;
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
            // On erasure chunks switch immediately, otherwise we can consume too much memory.
            Options_->ErasureCodec != ECodec::None || 
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
