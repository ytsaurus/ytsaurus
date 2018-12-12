#include "remote_copy_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/client/api/client.h>
#include <yt/client/api/config.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/ytlib/chunk_client/erasure_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/replication_writer.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_semaphore.h>

namespace NYT::NJobProxy {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NScheduler;
using namespace NJobTrackerClient::NProto;
using namespace NTableClient;
using namespace NApi;
using namespace NErasure;

using NJobTrackerClient::TStatistics;
using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static const auto& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyJob
    : public TJob
{
public:
    explicit TRemoteCopyJob(IJobHostPtr host)
        : TJob(host)
        , SchedulerJobSpecExt_(host->GetJobSpecHelper()->GetSchedulerJobSpecExt())
        , RemoteCopyJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext))
        , ReaderConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader)
        , WriterConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter)
        , RemoteCopyQueue_(New<TActionQueue>("RemoteCopy"))
        , CopySemaphore_(New<TAsyncSemaphore>(RemoteCopyJobSpecExt_.concurrency()))
    {
        YCHECK(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        YCHECK(SchedulerJobSpecExt_.output_table_specs_size() == 1);

        DataSliceDescriptors_ = UnpackDataSliceDescriptors(SchedulerJobSpecExt_.input_table_specs(0));

        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            for (const auto& inputChunkSpec : dataSliceDescriptor.ChunkSpecs) {
                YCHECK(!inputChunkSpec.has_lower_limit());
                YCHECK(!inputChunkSpec.has_upper_limit());
            }
        }

        BlockReadOptions_.WorkloadDescriptor = ReaderConfig_->WorkloadDescriptor;
        BlockReadOptions_.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        BlockReadOptions_.ReadSessionId = TReadSessionId::Create();
    }

    virtual void Initialize() override
    {
        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(SchedulerJobSpecExt_.output_table_specs(0).table_writer_options()));
        OutputChunkListId_ = FromProto<TChunkListId>(
            SchedulerJobSpecExt_.output_table_specs(0).chunk_list_id());

        auto remoteConnectionConfig = ConvertTo<NNative::TConnectionConfigPtr>(TYsonString(RemoteCopyJobSpecExt_.connection_config()));
        RemoteConnection_ = NNative::CreateConnection(remoteConnectionConfig);

        RemoteClient_ = RemoteConnection_->CreateNativeClient(TClientOptions(NSecurityClient::JobUserName));

        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            TotalChunkCount_ += dataSliceDescriptor.ChunkSpecs.size();
        }

        auto outputCellTag = CellTagFromId(OutputChunkListId_);
        MasterChannel_ = Host_->GetClient()->GetMasterChannelOrThrow(EMasterChannelKind::Leader, outputCellTag);
    }

    IInvokerPtr GetRemoteCopyInvoker() const
    {
        return RemoteCopyQueue_->GetInvoker();
    }

    void AttachChunksToChunkList(const std::vector<TChunkId>& chunksIds) const
    {
        TChunkServiceProxy proxy(MasterChannel_);

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        batchReq->set_suppress_upstream_sync(true);

        auto* req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), OutputChunkListId_);
        ToProto(req->mutable_child_ids(), chunksIds);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to attach chunks to chunk list");
    }

    void DoRun()
    {
        std::vector<TFuture<void>> chunkCopyResults;
        std::vector<TChunkId> outputChunkIds;

        PROFILE_TIMING ("/remote_copy_time") {
            for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
                for (const auto& inputChunkSpec : dataSliceDescriptor.ChunkSpecs) {
                    auto outputSessionId = CreateOutputChunk(inputChunkSpec);

                    auto result = BIND(&TRemoteCopyJob::CopyChunk, MakeStrong(this))
                        .AsyncVia(GetRemoteCopyInvoker())
                        .Run(inputChunkSpec, outputSessionId);

                    chunkCopyResults.push_back(result);
                    outputChunkIds.push_back(outputSessionId.ChunkId);
                }
            }

            WaitFor(Combine(chunkCopyResults))
                .ThrowOnError();

            YT_LOG_INFO("Attaching chunks to output chunk list (ChunkListId: %v)", OutputChunkListId_);
            AttachChunksToChunkList(outputChunkIds);
        }
    }

    virtual TJobResult Run() override
    {
        Host_->OnPrepared();

        auto runResult = BIND(&TRemoteCopyJob::DoRun, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run();

        WaitFor(runResult)
            .ThrowOnError();

        TJobResult result;
        ToProto(result.mutable_error(), TError());
        return result;
    }

    virtual void Cleanup() override
    { }

    virtual double GetProgress() const override
    {
        // Caution: progress calculated approximately (assuming all chunks have equal size).
        double currentProgress = TotalSize_ > 0 ? static_cast<double>(CopiedSize_) / TotalSize_ : 0.0;
        return (CopiedChunkCount_ + currentProgress) / TotalChunkCount_;
    }

    virtual ui64 GetStderrSize() const override
    {
        return 0;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkId_
            ? std::vector<TChunkId>(1, *FailedChunkId_)
            : std::vector<TChunkId>();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return { };
    }

    virtual void Interrupt() override
    {
        THROW_ERROR_EXCEPTION("Interrupting is not supported for this type of jobs")
            << TErrorAttribute("job_type", EJobType::RemoteCopy);
    }

    virtual TStatistics GetStatistics() const override
    {
        TStatistics result;
        result.AddSample("/data/input", DataStatistics_);
        result.AddSample(
            "/data/output/" + NYPath::ToYPathLiteral(0),
            DataStatistics_);
        return result;
    }

private:
    const TSchedulerJobSpecExt& SchedulerJobSpecExt_;
    const TRemoteCopyJobSpecExt& RemoteCopyJobSpecExt_;
    const TTableReaderConfigPtr ReaderConfig_;
    const TTableWriterConfigPtr WriterConfig_;

    std::vector<TDataSliceDescriptor> DataSliceDescriptors_;

    TTableWriterOptionsPtr WriterOptionsTemplate_;

    TChunkListId OutputChunkListId_;

    NNative::IConnectionPtr RemoteConnection_;
    NNative::IClientPtr RemoteClient_;

    IChannelPtr MasterChannel_;

    int CopiedChunkCount_ = 0;
    int TotalChunkCount_ = 0;
    i64 CopiedSize_ = 0;
    i64 TotalSize_ = 0;

    TDataStatistics DataStatistics_;

    std::optional<TChunkId> FailedChunkId_;

    const TActionQueuePtr RemoteCopyQueue_;
    TAsyncSemaphorePtr CopySemaphore_;

    TClientBlockReadOptions BlockReadOptions_;

    NChunkClient::TSessionId CreateOutputChunk(const TChunkSpec& inputChunkSpec)
    {
        auto writerOptions = CloneYsonSerializable(WriterOptionsTemplate_);
        writerOptions->ErasureCodec = NErasure::ECodec(inputChunkSpec.erasure_codec());
        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());

        return CreateChunk(
            Host_->GetClient(),
            CellTagFromId(OutputChunkListId_),
            writerOptions,
            transactionId,
            NullChunkListId,
            Logger);
    }

    void CopyChunk(const TChunkSpec& inputChunkSpec, const NChunkClient::TSessionId& outputSessionId)
    {
        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());

        YT_LOG_INFO("Copying chunk (InputChunkId: %v, OutputChunkId: %v)",
            inputChunkId, outputSessionId);

        auto erasureCodecId = NErasure::ECodec(inputChunkSpec.erasure_codec());

        auto inputReplicas = NYT::FromProto<TChunkReplicaList>(inputChunkSpec.replicas());

        // Copy chunk.
        TChunkInfo chunkInfo;
        TRefCountedChunkMetaPtr chunkMeta;
        TChunkReplicaList writtenReplicas;
        i64 totalChunkSize;

        if (erasureCodecId != NErasure::ECodec::None) {
            auto erasureCodec = NErasure::GetCodec(erasureCodecId);
            auto readers = CreateErasureAllPartsReaders(
                ReaderConfig_,
                New<TRemoteReaderOptions>(),
                RemoteClient_,
                Host_->GetInputNodeDirectory(),
                inputChunkId,
                inputReplicas,
                erasureCodec,
                Host_->GetBlockCache(),
                Host_->GetTrafficMeter(),
                Host_->GetInBandwidthThrottler(),
                Host_->GetOutRpsThrottler());

            chunkMeta = GetChunkMeta(readers.front());

            auto writers = CreateErasurePartWriters(
                WriterConfig_,
                New<TRemoteWriterOptions>(),
                outputSessionId,
                erasureCodec,
                New<TNodeDirectory>(),
                Host_->GetClient(),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());

            YCHECK(readers.size() == writers.size());

            auto erasurePlacementExt = GetProtoExtension<TErasurePlacementExt>(chunkMeta->extensions());

            // Compute an upper bound for total size.
            totalChunkSize =
                GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size() +
                erasurePlacementExt.parity_block_count() * erasurePlacementExt.parity_block_size() * erasurePlacementExt.parity_part_count();

            TotalSize_ += totalChunkSize;

            std::vector<TFuture<void>> copyResults;
            for (int index = 0; index < static_cast<int>(readers.size()); ++index) {
                std::vector<i64> blockSizes;
                int blockCount;
                if (index < erasureCodec->GetDataPartCount()) {
                    blockCount = erasurePlacementExt.part_infos(index).block_sizes_size();
                    for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
                        blockSizes.push_back(
                            erasurePlacementExt.part_infos(index).block_sizes(blockIndex));
                    }
                } else {
                    blockCount = erasurePlacementExt.parity_block_count();
                    blockSizes.resize(blockCount, erasurePlacementExt.parity_block_size());
                    blockSizes.back() = erasurePlacementExt.parity_last_block_size();
                }

                auto resultFuture = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
                    .AsyncVia(GetRemoteCopyInvoker())
                    .Run(readers[index], writers[index], blockSizes, chunkMeta);

                copyResults.push_back(resultFuture);
            }

            YT_LOG_INFO("Waiting for erasure parts data to be copied");

            WaitFor(Combine(copyResults))
                .ThrowOnError();

            i64 diskSpace = 0;
            for (int index = 0; index < static_cast<int>(readers.size()); ++index) {
                diskSpace += writers[index]->GetChunkInfo().disk_space();

                auto replicas = writers[index]->GetWrittenChunkReplicas();
                YCHECK(replicas.size() == 1);
                auto replica = TChunkReplica(
                    replicas.front().GetNodeId(),
                    index,
                    replicas.front().GetMediumIndex());

                writtenReplicas.push_back(replica);
            }

            chunkInfo.set_disk_space(diskSpace);
        } else {
            auto reader = CreateReplicationReader(
                ReaderConfig_,
                New<TRemoteReaderOptions>(),
                RemoteClient_,
                Host_->GetInputNodeDirectory(),
                Host_->LocalDescriptor(),
                inputChunkId,
                inputReplicas,
                Host_->GetBlockCache(),
                Host_->GetTrafficMeter(),
                Host_->GetInBandwidthThrottler(),
                Host_->GetOutRpsThrottler());

            chunkMeta = GetChunkMeta(reader);

            auto writer = CreateReplicationWriter(
                WriterConfig_,
                New<TRemoteWriterOptions>(),
                outputSessionId,
                TChunkReplicaList(),
                New<TNodeDirectory>(),
                Host_->GetClient(),
                GetNullBlockCache(),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());

            auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta->extensions());

            std::vector<i64> blockSizes;
            for (const auto& block : blocksExt.blocks()) {
                blockSizes.push_back(block.size());
            }

            totalChunkSize = GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size();

            auto result = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
                .AsyncVia(GetRemoteCopyInvoker())
                .Run(reader, writer, blockSizes, chunkMeta);

            YT_LOG_INFO("Waiting for chunk data to be copied");

            WaitFor(result)
                .ThrowOnError();

            chunkInfo = writer->GetChunkInfo();
            writtenReplicas = writer->GetWrittenChunkReplicas();
        }

        // Update data statistics.
        DataStatistics_.set_chunk_count(DataStatistics_.chunk_count() + 1);

        // Confirm chunk.
        YT_LOG_INFO("Confirming output chunk (ChunkId: %v)", outputSessionId);
        ConfirmChunkReplicas(outputSessionId, chunkInfo, writtenReplicas, chunkMeta);

        TotalSize_ -= totalChunkSize;
        CopiedChunkCount_ += 1;
    }

    void ConfirmChunkReplicas(
        const NChunkClient::TSessionId& outputSessionId,
        const TChunkInfo& chunkInfo,
        const TChunkReplicaList& writtenReplicas,
        const TRefCountedChunkMetaPtr& inputChunkMeta)
    {
        static const THashSet<int> masterMetaTags {
            TProtoExtensionTag<TMiscExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value
        };

        YCHECK(!writtenReplicas.empty());

        NChunkClient::NProto::TChunkMeta masterChunkMeta(*inputChunkMeta);
        FilterProtoExtensions(
            masterChunkMeta.mutable_extensions(),
            inputChunkMeta->extensions(),
            masterMetaTags);

        TChunkServiceProxy proxy(MasterChannel_);

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        batchReq->set_suppress_upstream_sync(true);

        auto* req = batchReq->add_confirm_chunk_subrequests();
        ToProto(req->mutable_chunk_id(), outputSessionId.ChunkId);
        *req->mutable_chunk_info() = chunkInfo;
        *req->mutable_chunk_meta() = masterChunkMeta;
        NYT::ToProto(req->mutable_replicas(), writtenReplicas);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            outputSessionId.ChunkId);
    }

    void DoCopy(
        IChunkReaderPtr reader,
        IChunkWriterPtr writer,
        const std::vector<i64>& blockSizes,
        const TRefCountedChunkMetaPtr& meta)
    {
        auto acquireSemaphoreGuard = [&] () {
            while (true) {
                auto guard = TAsyncSemaphoreGuard::TryAcquire(CopySemaphore_);
                if (guard) {
                    return guard;
                }

                WaitFor(CopySemaphore_->GetReadyEvent())
                    .ThrowOnError();
            }
        };

        auto semaphoreGuard = acquireSemaphoreGuard();

        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error opening writer");

        int blockCount = static_cast<int>(blockSizes.size());

        int blockIndex = 0;
        while (blockIndex < blockCount) {
            int beginBlockIndex = blockIndex;
            int endBlockIndex = blockIndex;
            i64 sizeToRead = 0;

            while (endBlockIndex < blockCount && sizeToRead <= RemoteCopyJobSpecExt_.block_buffer_size()) {
                sizeToRead += blockSizes[endBlockIndex];
                endBlockIndex += 1;
            }

            // This can happen if we encounter block which is bigger than block buffer size.
            // In this case at least one block should be read (this memory overhead is taken
            // into account in operation controller)
            if (endBlockIndex == beginBlockIndex) {
                endBlockIndex += 1;
            }

            auto asyncResult = reader->ReadBlocks(
                BlockReadOptions_,
                beginBlockIndex,
                endBlockIndex - beginBlockIndex);

            auto result = WaitFor(asyncResult);

            if (!result.IsOK()) {
                FailedChunkId_ = reader->GetChunkId();
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reading blocks");
            }

            auto blocks = result.Value();

            i64 blocksSize = 0;
            for (const auto& block : blocks) {
                blocksSize += block.Size();
            }

            CopiedSize_ += blocksSize;

            if (!writer->WriteBlocks(blocks)) {
                auto result = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error writing block");
            }

            DataStatistics_.set_compressed_data_size(DataStatistics_.compressed_data_size() + blocksSize);

            blockIndex += blocks.size();
        }

        {
            auto result = WaitFor(writer->Close(meta));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error closing chunk");
        }
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    TRefCountedChunkMetaPtr GetChunkMeta(const IChunkReaderPtr& reader)
    {
        auto asyncResult = reader->GetMeta(BlockReadOptions_);
        auto result = WaitFor(asyncResult);
        if (!result.IsOK()) {
            FailedChunkId_ = reader->GetChunkId();
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to get chunk meta");
        }
        return result.Value();
    }
};

IJobPtr CreateRemoteCopyJob(IJobHostPtr host)
{
    return New<TRemoteCopyJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
