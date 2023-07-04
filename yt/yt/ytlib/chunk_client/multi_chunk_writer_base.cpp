#include "multi_chunk_writer_base.h"
#include "private.h"
#include "chunk_writer.h"
#include "config.h"
#include "confirming_writer.h"
#include "dispatcher.h"
#include "deferred_chunk_meta.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NErasure;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TNontemplateMultiChunkWriterBase::TNontemplateMultiChunkWriterBase(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NNative::IClientPtr client,
    TString localHostName,
    TCellTag cellTag,
    TTransactionId transactionId,
    TChunkListId parentChunkListId,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
    : Logger(ChunkClientLogger)
    , Client_(client)
    , Config_(config)
    , Options_(options)
    , CellTag_(cellTag)
    , LocalHostName_(std::move(localHostName))
    , TransactionId_(transactionId)
    , ParentChunkListId_(parentChunkListId)
    , Throttler_(throttler)
    , BlockCache_(blockCache)
    , TrafficMeter_(trafficMeter)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Options_);

    Logger.AddTag("TransactionId: %v", TransactionId_);
}

void TNontemplateMultiChunkWriterBase::Init()
{
    InitSession();
}

TFuture<void> TNontemplateMultiChunkWriterBase::Close()
{
    YT_VERIFY(!Closing_);
    YT_VERIFY(ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK());

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

const std::vector<TChunkSpec>& TNontemplateMultiChunkWriterBase::GetWrittenChunkSpecs() const
{
    return WrittenChunkSpecs_;
}

const TChunkWithReplicasList& TNontemplateMultiChunkWriterBase::GetWrittenChunkWithReplicasList() const
{
    return WrittenChunkWithReplicasList_;
}

TDataStatistics TNontemplateMultiChunkWriterBase::GetDataStatistics() const
{
    auto guard = Guard(SpinLock_);
    auto result = DataStatistics_;
    if (CurrentSession_.IsActive()) {
        result += CurrentSession_.TemplateWriter->GetDataStatistics();
    }
    return result;
}

TCodecStatistics TNontemplateMultiChunkWriterBase::GetCompressionStatistics() const
{
    auto guard = Guard(SpinLock_);
    auto result = CodecStatistics;
    if (CurrentSession_.IsActive()) {
        result += CurrentSession_.TemplateWriter->GetCompressionStatistics();
    }
    return result;
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
    if (CurrentSession_.TemplateWriter->GetCompressedDataSize() == 0) {
        return;
    }

    WaitFor(CurrentSession_.TemplateWriter->Close())
        .ThrowOnError();

    {
        auto chunkId = CurrentSession_.UnderlyingWriter->GetChunkId();
        auto writtenChunkReplicas = CurrentSession_.UnderlyingWriter->GetWrittenChunkReplicas();

        auto& chunkSpec = WrittenChunkSpecs_.emplace_back();
        ToProto(chunkSpec.mutable_chunk_id(), chunkId);
        for (auto replica : writtenChunkReplicas) {
            chunkSpec.add_legacy_replicas(ToProto<ui32>(replica.ToChunkReplica()));
            chunkSpec.add_replicas(ToProto<ui64>(replica));
        }
        if (Options_->TableIndex != TMultiChunkWriterOptions::InvalidTableIndex) {
            chunkSpec.set_table_index(Options_->TableIndex);
        }

        const auto& chunkMeta = *CurrentSession_.TemplateWriter->GetMeta();
        *chunkSpec.mutable_chunk_meta() = chunkMeta;

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
        chunkSpec.set_erasure_codec(miscExt.erasure_codec());
        chunkSpec.set_striped_erasure(miscExt.striped_erasure());

        WrittenChunkWithReplicasList_.emplace_back(chunkId, std::move(writtenChunkReplicas));
    }

    {
        auto guard = Guard(SpinLock_);
        DataStatistics_ += CurrentSession_.TemplateWriter->GetDataStatistics();
        CodecStatistics += CurrentSession_.TemplateWriter->GetCompressionStatistics();
        CurrentSession_.Reset();
    }
}

void TNontemplateMultiChunkWriterBase::InitSession()
{
    auto guard = Guard(SpinLock_);

    CurrentSession_.UnderlyingWriter = CreateConfirmingWriter(
        Config_,
        Options_,
        CellTag_,
        TransactionId_,
        ParentChunkListId_,
        Client_,
        LocalHostName_,
        BlockCache_,
        TrafficMeter_,
        Throttler_);

    CurrentSession_.TemplateWriter = CreateTemplateWriter(CurrentSession_.UnderlyingWriter);

    SwitchingSession_ = false;
}

bool TNontemplateMultiChunkWriterBase::TrySwitchSession()
{
    if (CurrentSession_.TemplateWriter->IsCloseDemanded()) {
        YT_LOG_DEBUG("Switching to next chunk due to chunk writer demand");

        SwitchSession();
        return true;
    }

    if (CurrentSession_.TemplateWriter->GetMetaSize() > Config_->MaxMetaSize) {
        YT_LOG_DEBUG("Switching to next chunk: meta is too large (ChunkMetaSize: %v)",
            CurrentSession_.TemplateWriter->GetMetaSize());

        SwitchSession();
        return true;
    }

    if (CurrentSession_.TemplateWriter->GetDataWeight() > Config_->DesiredChunkWeight) {
        YT_LOG_DEBUG("Switching to next chunk: data weight is too large (DataWeight: %v)",
            CurrentSession_.TemplateWriter->GetDataWeight());

        SwitchSession();
        return true;
    }

    if (CurrentSession_.TemplateWriter->GetCompressedDataSize() > Config_->DesiredChunkSize) {
        if (Options_->ErasureCodec != ECodec::None ||
            CurrentSession_.TemplateWriter->GetCompressedDataSize() > 2 * Config_->DesiredChunkSize)
        {
            YT_LOG_DEBUG("Switching to next chunk: compressed data size is too large (CurrentSessionSize: %v, DesiredChunkSize: %v)",
                CurrentSession_.TemplateWriter->GetCompressedDataSize(),
                Config_->DesiredChunkSize);

            SwitchSession();
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
