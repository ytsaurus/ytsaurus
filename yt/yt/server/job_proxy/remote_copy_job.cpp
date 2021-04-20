#include "remote_copy_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/erasure_part_reader.h>
#include <yt/yt/ytlib/chunk_client/erasure_part_writer.h>
#include <yt/yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/replication_writer.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

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

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyJob
    : public TJob
{
public:
    explicit TRemoteCopyJob(IJobHost* host)
        : TJob(host)
        , SchedulerJobSpecExt_(host->GetJobSpecHelper()->GetSchedulerJobSpecExt())
        , RemoteCopyJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext))
        , ReaderConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader)
        , WriterConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter)
        , RemoteCopyQueue_(New<TActionQueue>("RemoteCopy"))
        , CopySemaphore_(New<TAsyncSemaphore>(RemoteCopyJobSpecExt_.concurrency()))
    {
        YT_VERIFY(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);

        DataSliceDescriptors_ = UnpackDataSliceDescriptors(SchedulerJobSpecExt_.input_table_specs(0));

        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            for (const auto& inputChunkSpec : dataSliceDescriptor.ChunkSpecs) {
                YT_VERIFY(!inputChunkSpec.has_lower_limit());
                YT_VERIFY(!inputChunkSpec.has_upper_limit());
            }
        }

        ChunkReadOptions_.WorkloadDescriptor = ReaderConfig_->WorkloadDescriptor;
        ChunkReadOptions_.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        ChunkReadOptions_.ReadSessionId = TReadSessionId::Create();
        // We are not ready for reordering here.
        WriterConfig_->EnableBlockReordering = false;
    }

    virtual void Initialize() override
    {
        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(SchedulerJobSpecExt_.output_table_specs(0).table_writer_options()));
        OutputChunkListId_ = FromProto<TChunkListId>(
            SchedulerJobSpecExt_.output_table_specs(0).chunk_list_id());

        auto remoteConnectionConfig = ConvertTo<NNative::TConnectionConfigPtr>(TYsonString(RemoteCopyJobSpecExt_.connection_config()));
        RemoteConnection_ = NNative::CreateConnection(remoteConnectionConfig);

        RemoteClient_ = RemoteConnection_->CreateNativeClient(TClientOptions::FromUser(Host_->GetJobUserName()));

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

        YT_PROFILE_TIMING("/job_proxy/remote_copy_time") {
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

            WaitFor(AllSucceeded(chunkCopyResults))
                .ThrowOnError();

            {
                auto finalizeResult = WaitFor(AllSucceeded(ChunkFinalizationResults_));
                THROW_ERROR_EXCEPTION_IF_FAILED(finalizeResult, "Error finalizing chunk");
            }

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

        if (IsTableDynamic()) {
            auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            ToProto(schedulerResultExt->mutable_output_chunk_specs(), WrittenChunks_);
        }

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

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkIds_;
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

    std::vector<TChunkId> FailedChunkIds_;

    const TActionQueuePtr RemoteCopyQueue_;
    TAsyncSemaphorePtr CopySemaphore_;

    TClientChunkReadOptions ChunkReadOptions_;

    std::vector<TFuture<void>> ChunkFinalizationResults_;

    // For dynamic tables only.
    std::vector<TChunkSpec> WrittenChunks_;

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

    void CopyChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        // Delay for testing purposes.
        // COMPAT(gritukan)
        if (RemoteCopyJobSpecExt_.has_delay_in_copy_chunk()) {
            auto delayInCopyChunk = FromProto<TDuration>(RemoteCopyJobSpecExt_.delay_in_copy_chunk());
            if (delayInCopyChunk > TDuration::Zero()) {
                YT_LOG_DEBUG("Sleeping in CopyChunk (DelayInCopyChunk: %v)", delayInCopyChunk);
                Sleep(delayInCopyChunk);
            }
        }

        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());

        YT_LOG_INFO("Copying chunk (InputChunkId: %v, OutputChunkId: %v)",
            inputChunkId, outputSessionId);

        auto erasureCodecId = NErasure::ECodec(inputChunkSpec.erasure_codec());
        if (erasureCodecId != NErasure::ECodec::None) {
            CopyErasureChunk(inputChunkSpec, outputSessionId);
        } else {
            CopyRegularChunk(inputChunkSpec, outputSessionId);
        }

        // Update data statistics.
        DataStatistics_.set_chunk_count(DataStatistics_.chunk_count() + 1);
    }

    void CopyErasureChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        auto cancelableContext = New<TCancelableContext>();
        auto suspendableInvoker = CreateSuspendableInvoker(GetRemoteCopyInvoker());
        auto cancelableInvoker = cancelableContext->CreateInvoker(suspendableInvoker);

        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());
        auto erasureCodecId = NErasure::ECodec(inputChunkSpec.erasure_codec());
        auto inputReplicas = NYT::FromProto<TChunkReplicaList>(inputChunkSpec.replicas());

        TDeferredChunkMetaPtr chunkMeta;

        auto erasureCodec = NErasure::GetCodec(erasureCodecId);
        auto readers = CreateAllErasurePartReaders(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            RemoteClient_,
            Host_->GetInputNodeDirectory(),
            inputChunkId,
            inputReplicas,
            erasureCodec,
            Host_->GetReaderBlockCache(),
            Host_->GetTrafficMeter(),
            Host_->GetInBandwidthThrottler(),
            Host_->GetOutRpsThrottler());

        chunkMeta = GetChunkMeta(readers);

        // We do not support node reallocation for erasure chunks.
        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;
        auto writers = CreateAllErasurePartWriters(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            erasureCodec,
            New<TNodeDirectory>(),
            Host_->GetClient(),
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler());
        YT_VERIFY(readers.size() == writers.size());

        auto erasurePlacementExt = GetProtoExtension<TErasurePlacementExt>(chunkMeta->extensions());

        int parityPartBlockCount = 0;
        for (int count : erasurePlacementExt.parity_block_count_per_stripe()) {
            parityPartBlockCount += count;
        }

        // Compute an upper bound for total size.
        i64 totalChunkSize = GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size() +
            parityPartBlockCount * erasurePlacementExt.parity_block_size() * erasurePlacementExt.parity_part_count();

        TotalSize_ += totalChunkSize;

        std::vector<TFuture<void>> copyFutures;
        copyFutures.reserve(readers.size());
        for (int index = 0; index < static_cast<int>(readers.size()); ++index) {
            std::vector<i64> blockSizes;
            if (index < erasureCodec->GetDataPartCount()) {
                int blockCount = erasurePlacementExt.part_infos(index).block_sizes_size();
                for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
                    blockSizes.push_back(
                        erasurePlacementExt.part_infos(index).block_sizes(blockIndex));
                }
            } else {
                for (int stripeIndex = 0; stripeIndex < erasurePlacementExt.parity_block_count_per_stripe_size(); stripeIndex++) {
                    blockSizes.insert(
                        blockSizes.end(),
                        erasurePlacementExt.parity_block_count_per_stripe(stripeIndex),
                        erasurePlacementExt.parity_block_size());
                    blockSizes.back() = erasurePlacementExt.parity_last_block_size_per_stripe(stripeIndex);
                }
            }

            auto copyFuture = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
                .AsyncVia(cancelableInvoker)
                .Run(readers[index], writers[index], blockSizes, chunkMeta);

            copyFutures.push_back(copyFuture);
        }

        auto repairChunk = RemoteCopyJobSpecExt_.repair_erasure_chunks();

        YT_LOG_INFO("Waiting for erasure parts data to be copied (RepairChunk: %v)",
            repairChunk);

        TPartIndexList erasedPartIndicies;

        if (repairChunk) {
            auto copyStarted = TInstant::Now();

            WaitUntilErasureChunkCanBeRepaired(erasureCodec, &copyFutures);

            // COMPAT(gritukan)
            TDuration erasureChunkRepairDelay;
            if (RemoteCopyJobSpecExt_.has_erasure_chunk_repair_delay()) {
                erasureChunkRepairDelay = FromProto<TDuration>(RemoteCopyJobSpecExt_.erasure_chunk_repair_delay());
            } else {
                erasureChunkRepairDelay = TDuration::Max() / 2;
            }

            auto copyTimeElapsed = TInstant::Now();

            // copyTimeElapsed - copyStarted <= erasureChunkRepairDelay.
            if (copyTimeElapsed <= copyStarted + erasureChunkRepairDelay) {
                erasureChunkRepairDelay -= (copyTimeElapsed - copyStarted);
            } else {
                erasureChunkRepairDelay = TDuration::Zero();
            }

            std::vector<TFuture<void>> copyFutureWithTimeouts;
            copyFutureWithTimeouts.reserve(copyFutures.size());
            for (const auto& copyFuture : copyFutures) {
                copyFutureWithTimeouts.push_back(copyFuture.WithTimeout(erasureChunkRepairDelay));
            }

            // Wait for all parts were copied within timeout.
            auto copyResults = WaitFor(AllSet(copyFutureWithTimeouts))
                .ValueOrThrow();

            cancelableContext->Cancel(TError("Erasure part repair started"));

            // Wait until all part copyings terminated.
            WaitFor(suspendableInvoker->Suspend())
                .ThrowOnError();

            std::vector<TFuture<void>> closeReplicaWriterResults;
            for (int partIndex = 0; partIndex < copyFutures.size(); ++partIndex) {
                auto copyResult = copyResults[partIndex];
                if (!copyResult.IsOK()) {
                    erasedPartIndicies.push_back(partIndex);
                    // NB: We should destroy replication writer to cancel it.
                    writers[partIndex].Reset();
                } else {
                    const auto& writer = writers[partIndex];
                    closeReplicaWriterResults.push_back(writer->Close(chunkMeta));
                }
            }

            WaitFor(AllSucceeded(closeReplicaWriterResults))
                .ThrowOnError();
        } else {
            WaitFor(AllSucceeded(copyFutures))
                .ThrowOnError();

            std::vector<TFuture<void>> closeReplicaWriterResults;
            closeReplicaWriterResults.reserve(writers.size());
            for (const auto& writer : writers) {
                closeReplicaWriterResults.push_back(writer->Close(chunkMeta));
            }
            WaitFor(AllSucceeded(closeReplicaWriterResults))
                .ThrowOnError();
        }

        if (!erasedPartIndicies.empty()) {
            RepairErasureChunk(
                outputSessionId,
                erasureCodec,
                erasedPartIndicies,
                &writers);
        } else {
            YT_LOG_DEBUG("All the parts were copied successfully");
        }

        ChunkFinalizationResults_.push_back(BIND(&TRemoteCopyJob::FinalizeErasureChunk, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(writers, erasedPartIndicies, chunkMeta, outputSessionId));

        TotalSize_ -= totalChunkSize;
        CopiedChunkCount_ += 1;
    }

    //! Waits until enough parts were copied to perform repair.
    void WaitUntilErasureChunkCanBeRepaired(
        NErasure::ICodec* erasureCodec,
        std::vector<TFuture<void>>* copyFutures)
    {
        // This promise is set when repair can be started.
        TPromise<void> canStartRepair = NewPromise<void>();

        // Set of parts that were not copied yet.
        TPartIndexSet erasedPartSet;

        // Set of parts that were copied unsuccessfully.
        TPartIndexSet failedPartSet;

        std::vector<TError> copyErrors;

        std::vector<TFutureCallbackCookie> callbackCookies;
        callbackCookies.reserve(copyFutures->size());

        for (int partIndex = 0; partIndex < copyFutures->size(); ++partIndex) {
            erasedPartSet.set(partIndex);

            auto& copyFuture = (*copyFutures)[partIndex];
            auto cookie = copyFuture.Subscribe(BIND([&, partIndex, Logger = Logger] (const TError& error) {
                if (error.IsOK()) {
                    erasedPartSet.reset(partIndex);
                    if (erasureCodec->CanRepair(erasedPartSet)) {
                        canStartRepair.TrySet();
                    }
                } else {
                    failedPartSet.set(partIndex);
                    copyErrors.push_back(error);
                    // Chunk cannot be repaired, this situation is unrecoverable.
                    if (!erasureCodec->CanRepair(failedPartSet)) {
                        canStartRepair.TrySet(TError("Cannot repair erasure chunk")
                            << copyErrors);
                    }
                }
            }));
            callbackCookies.push_back(cookie);
        }

        WaitFor(canStartRepair.ToFuture())
            .ThrowOnError();

        for (int partIndex = 0; partIndex < copyFutures->size(); ++partIndex) {
            auto& copyFuture = (*copyFutures)[partIndex];
            auto callbackCookie = callbackCookies[partIndex];
            copyFuture.Unsubscribe(callbackCookie);
        }
    }

    void RepairErasureChunk(
        NChunkClient::TSessionId outputSessionId,
        NErasure::ICodec* erasureCodec,
        const TPartIndexList& erasedPartIndicies,
        std::vector<IChunkWriterPtr>* partWriters)
    {
        auto repairPartIndicies = *erasureCodec->GetRepairIndices(erasedPartIndicies);

        YT_LOG_INFO("Failed to copy some of the chunk parts, starting repair (ErasedPartIndicies: %v, RepairPartIndicies: %v)",
            erasedPartIndicies,
            repairPartIndicies);

        TChunkReplicaList localChunkReplicas;
        localChunkReplicas.resize(repairPartIndicies.size());
        for (auto repairPartIndex : repairPartIndicies) {
            auto replicas = (*partWriters)[repairPartIndex]->GetWrittenChunkReplicas();
            YT_VERIFY(replicas.size() == 1);
            auto replica = TChunkReplica(replicas.front().GetNodeId(), repairPartIndex);
            localChunkReplicas.push_back(replica);
        }

        auto repairPartReaders = CreateErasurePartReaders(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            Host_->GetClient(),
            Host_->GetInputNodeDirectory(),
            outputSessionId.ChunkId,
            localChunkReplicas,
            erasureCodec,
            repairPartIndicies,
            Host_->GetReaderBlockCache(),
            Host_->GetTrafficMeter(),
            Host_->GetInBandwidthThrottler(),
            Host_->GetOutRpsThrottler());
        YT_VERIFY(repairPartReaders.size() == repairPartIndicies.size());

        auto erasedPartWriters = CreateErasurePartWriters(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            erasureCodec,
            New<TNodeDirectory>(),
            Host_->GetClient(),
            erasedPartIndicies,
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler());
        YT_VERIFY(erasedPartWriters.size() == erasedPartIndicies.size());

        WaitFor(RepairErasedParts(
            erasureCodec,
            erasedPartIndicies,
            repairPartReaders,
            erasedPartWriters,
            TClientChunkReadOptions()))
            .ThrowOnError();

        for (int index = 0; index < erasedPartIndicies.size(); ++index) {
            (*partWriters)[erasedPartIndicies[index]] = erasedPartWriters[index];
        }
    }

    void FinalizeErasureChunk(
        const std::vector<IChunkWriterPtr>& writers,
        const TPartIndexList& erasedPartIndicies,
        const TDeferredChunkMetaPtr& chunkMeta,
        NChunkClient::TSessionId outputSessionId)
    {
        TChunkInfo chunkInfo;
        TChunkReplicaWithMediumList writtenReplicas;

        i64 diskSpace = 0;
        for (int index = 0; index < static_cast<int>(writers.size()); ++index) {
            diskSpace += writers[index]->GetChunkInfo().disk_space();
            auto replicas = writers[index]->GetWrittenChunkReplicas();
            YT_VERIFY(replicas.size() == 1);
            auto replica = TChunkReplicaWithMedium(
                replicas.front().GetNodeId(),
                index,
                replicas.front().GetMediumIndex());

            writtenReplicas.push_back(replica);
        }

        chunkInfo.set_disk_space(diskSpace);

        ConfirmChunkReplicas(outputSessionId, chunkInfo, writtenReplicas, chunkMeta);
    }

    void CopyRegularChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());
        auto inputReplicas = NYT::FromProto<TChunkReplicaList>(inputChunkSpec.replicas());

        TDeferredChunkMetaPtr chunkMeta;

        auto reader = CreateReplicationReader(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            RemoteClient_,
            Host_->GetInputNodeDirectory(),
            Host_->LocalDescriptor(),
            std::nullopt,
            inputChunkId,
            inputReplicas,
            Host_->GetReaderBlockCache(),
            Host_->GetTrafficMeter(),
            /* nodeStatusDirectory */ nullptr,
            Host_->GetInBandwidthThrottler(),
            Host_->GetOutRpsThrottler());

        chunkMeta = GetChunkMeta({reader});

        auto writer = CreateReplicationWriter(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            TChunkReplicaWithMediumList(),
            New<TNodeDirectory>(),
            Host_->GetClient(),
            Host_->GetWriterBlockCache(),
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler());

        auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta->extensions());

        std::vector<i64> blockSizes;
        for (const auto& block : blocksExt.blocks()) {
            blockSizes.push_back(block.size());
        }

        i64 totalChunkSize = GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size();

        auto result = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(reader, writer, blockSizes, chunkMeta);

        YT_LOG_INFO("Waiting for chunk data to be copied");

        WaitFor(result)
            .ThrowOnError();

        ChunkFinalizationResults_.push_back(BIND(&TRemoteCopyJob::FinalizeRegularChunk, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(writer, chunkMeta, outputSessionId));

        TotalSize_ -= totalChunkSize;
        CopiedChunkCount_ += 1;
    }

    void FinalizeRegularChunk(
        const IChunkWriterPtr& writer,
        const TDeferredChunkMetaPtr& chunkMeta,
        NChunkClient::TSessionId outputSessionId)
    {
        WaitFor(writer->Close(chunkMeta))
            .ThrowOnError();
        TChunkInfo chunkInfo = writer->GetChunkInfo();
        TChunkReplicaWithMediumList writtenReplicas = writer->GetWrittenChunkReplicas();
        ConfirmChunkReplicas(outputSessionId, chunkInfo, writtenReplicas, chunkMeta);
    }

    void ConfirmChunkReplicas(
        NChunkClient::TSessionId outputSessionId,
        const TChunkInfo& chunkInfo,
        const TChunkReplicaWithMediumList& writtenReplicas,
        const TRefCountedChunkMetaPtr& inputChunkMeta)
    {
        YT_LOG_INFO("Confirming output chunk (ChunkId: %v)", outputSessionId.ChunkId);

        static const THashSet<int> masterMetaTags {
            TProtoExtensionTag<TMiscExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value
        };

        YT_VERIFY(!writtenReplicas.empty());

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

        if (IsTableDynamic()) {
            TChunkSpec chunkSpec;
            *chunkSpec.mutable_chunk_meta() = masterChunkMeta;
            ToProto(chunkSpec.mutable_chunk_id(), outputSessionId.ChunkId);
            WrittenChunks_.push_back(chunkSpec);
        }
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
                ChunkReadOptions_,
                beginBlockIndex,
                endBlockIndex - beginBlockIndex);

            auto result = WaitFor(asyncResult);

            if (!result.IsOK()) {
                FailedChunkIds_.push_back(reader->GetChunkId());
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
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    TDeferredChunkMetaPtr GetChunkMeta(const std::vector<IChunkReaderPtr>& readers)
    {
        // In erasure chunks some of the parts might be unavailable, but they all have the same meta,
        // so we try to get meta from all of the readers simultaneously.
        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncResults;
        asyncResults.reserve(readers.size());
        for (const auto& reader : readers) {
            asyncResults.push_back(reader->GetMeta(ChunkReadOptions_));
        }

        auto result = WaitFor(AnySucceeded(asyncResults));
        if (!result.IsOK()) {
            FailedChunkIds_.reserve(readers.size());
            for (const auto& reader : readers) {
                FailedChunkIds_.push_back(reader->GetChunkId());
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to get chunk meta");
        }
        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->CopyFrom(*result.Value());
        return deferredChunkMeta;
    }

    bool IsTableDynamic() const
    {
        return SchedulerJobSpecExt_.output_table_specs(0).dynamic();
    }
};

IJobPtr CreateRemoteCopyJob(IJobHost* host)
{
    return New<TRemoteCopyJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
