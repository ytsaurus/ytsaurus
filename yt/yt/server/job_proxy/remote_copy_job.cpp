#include "remote_copy_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
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
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

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
using namespace NControllerAgent;
using namespace NScheduler;
using namespace NControllerAgent::NProto;
using namespace NTableClient;
using namespace NApi;
using namespace NErasure;
using namespace NTracing;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyJob
    : public TJob
{
public:
    explicit TRemoteCopyJob(IJobHostPtr host)
        : TJob(std::move(host))
        , JobSpecExt_(Host_->GetJobSpecHelper()->GetJobSpecExt())
        , RemoteCopyJobSpecExt_(Host_->GetJobSpecHelper()->GetJobSpec().GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext))
        , ReaderConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader)
        , WriterConfig_(CloneYsonStruct(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter))
        , RemoteCopyQueue_(New<TActionQueue>("RemoteCopy"))
        , CopySemaphore_(New<TAsyncSemaphore>(RemoteCopyJobSpecExt_.concurrency()))
        , InputTraceContext_(CreateTraceContextFromCurrent("TableReader"))
        , InputFinishGuard_(InputTraceContext_)
        , OutputTraceContext_(CreateTraceContextFromCurrent("TableWriter"))
        , OutputFinishGuard_(OutputTraceContext_)
    {
        YT_VERIFY(JobSpecExt_.input_table_specs_size() == 1);
        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);

        DataSliceDescriptors_ = Host_->GetJobSpecHelper()->UnpackDataSliceDescriptors();

        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            for (const auto& inputChunkSpec : dataSliceDescriptor.ChunkSpecs) {
                YT_VERIFY(!inputChunkSpec.has_lower_limit());
                YT_VERIFY(!inputChunkSpec.has_upper_limit());
            }
        }

        ReadBlocksOptions_.ClientOptions.WorkloadDescriptor = ReaderConfig_->WorkloadDescriptor;
        ReadBlocksOptions_.ClientOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        ReadBlocksOptions_.ClientOptions.ReadSessionId = TReadSessionId::Create();

        // We are not ready for reordering here.
        WriterConfig_->EnableBlockReordering = false;

        for (const auto& mapping : RemoteCopyJobSpecExt_.hunk_chunk_id_mapping()) {
            EmplaceOrCrash(
                HunkChunkIdMapping_,
                FromProto<TChunkId>(mapping.input_hunk_chunk_id()),
                FromProto<TChunkId>(mapping.output_hunk_chunk_id()));
        }
    }

    void PopulateInputNodeDirectory() const override
    {
        RemoteConnection_->GetNodeDirectory()->MergeFrom(
            Host_->GetJobSpecHelper()->GetJobSpecExt().input_node_directory());
    }

    void Initialize() override
    {
        auto remoteConnectionConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(TYsonString(RemoteCopyJobSpecExt_.connection_config()));
        RemoteConnection_ = Host_->CreateNativeConnection(remoteConnectionConfig);

        TJob::Initialize();

        TExtraChunkTags extraChunkTags;
        const auto& tableSpec = JobSpecExt_.input_table_specs()[0];
        if (tableSpec.chunk_specs_size() != 0) {
            extraChunkTags.ErasureCodec = NErasure::ECodec(tableSpec.chunk_specs()[0].erasure_codec());
        }

        auto dataSourceDirectory = Host_->GetJobSpecHelper()->GetDataSourceDirectory();
        YT_VERIFY(std::ssize(dataSourceDirectory->DataSources()) == 1);
        PackBaggageForChunkReader(InputTraceContext_, dataSourceDirectory->DataSources()[0], extraChunkTags);

        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(JobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            PackBaggageForChunkWriter(OutputTraceContext_, dataSinkDirectory->DataSinks()[0], extraChunkTags);
        }

        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(JobSpecExt_.output_table_specs(0).table_writer_options()));

        OutputChunkListId_ = FromProto<TChunkListId>(
            JobSpecExt_.output_table_specs(0).chunk_list_id());

        WriterConfig_->UploadReplicationFactor = std::min(
            WriterConfig_->UploadReplicationFactor,
            WriterOptionsTemplate_->ReplicationFactor);

        RemoteClient_ = RemoteConnection_->CreateNativeClient(TClientOptions::FromUser(Host_->GetAuthenticatedUser()));

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
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
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

        YT_LOG_INFO("Attaching chunks to output chunk list (ChunkListId: %v)",
            OutputChunkListId_);
        AttachChunksToChunkList(outputChunkIds);
    }

    TJobResult Run() override
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
            auto* jobResultExt = result.MutableExtension(TJobResultExt::job_result_ext);
            ToProto(jobResultExt->mutable_output_chunk_specs(), WrittenChunks_);

            for (const auto& mapping : ResultHunkChunkIdMapping_) {
                auto* protoMapping = jobResultExt->add_hunk_chunk_id_mapping();
                ToProto(protoMapping->mutable_input_hunk_chunk_id(), mapping.first);
                ToProto(protoMapping->mutable_output_hunk_chunk_id(), mapping.second);
            }
        }

        return result;
    }

    void Cleanup() override
    { }

    void PrepareArtifacts() override
    { }

    double GetProgress() const override
    {
        // Caution: progress calculated approximately (assuming all chunks have equal size).
        double currentProgress = TotalSize_ > 0 ? static_cast<double>(CopiedSize_) / TotalSize_ : 0.0;
        return (CopiedChunkCount_ + currentProgress) / TotalChunkCount_;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkIds_;
    }

    TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return { };
    }

    void Interrupt() override
    {
        THROW_ERROR_EXCEPTION("Interrupting is not supported for this type of jobs")
            << TErrorAttribute("job_type", EJobType::RemoteCopy);
    }

    TStatistics GetStatistics() const override
    {
        return {
            .ChunkReaderStatistics = ReadBlocksOptions_.ClientOptions.ChunkReaderStatistics,
            .ChunkWriterStatistics = {WriteBlocksOptions_.ClientOptions.ChunkWriterStatistics},
            .TotalInputStatistics = {
                .DataStatistics = DataStatistics_,
            },
            .OutputStatistics = {{
                .DataStatistics = DataStatistics_,
            }},
        };
    }

private:
    const TJobSpecExt& JobSpecExt_;
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
    const TAsyncSemaphorePtr CopySemaphore_;

    IChunkReader::TReadBlocksOptions ReadBlocksOptions_;
    IChunkWriter::TWriteBlocksOptions WriteBlocksOptions_;

    std::vector<TFuture<void>> ChunkFinalizationResults_;

    // For dynamic tables only.
    std::vector<TChunkSpec> WrittenChunks_;

    THashMap<TChunkId, TChunkId> HunkChunkIdMapping_;
    THashMap<TChunkId, TChunkId> ResultHunkChunkIdMapping_;

    TTraceContextPtr InputTraceContext_;
    TTraceContextFinishGuard InputFinishGuard_;
    TTraceContextPtr OutputTraceContext_;
    TTraceContextFinishGuard OutputFinishGuard_;

    NChunkClient::TSessionId CreateOutputChunk(const TChunkSpec& inputChunkSpec)
    {
        auto writerOptions = CloneYsonStruct(WriterOptionsTemplate_);
        writerOptions->ErasureCodec = FromProto<NErasure::ECodec>(inputChunkSpec.erasure_codec());

        auto transactionId = FromProto<TTransactionId>(JobSpecExt_.output_transaction_id());

        return CreateChunk(
            Host_->GetClient(),
            CellTagFromId(OutputChunkListId_),
            writerOptions,
            transactionId,
            NullChunkListId,
            Logger());
    }

    void CopyChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        // Delay for testing purposes.
        // COMPAT(gritukan)
        if (RemoteCopyJobSpecExt_.has_delay_in_copy_chunk()) {
            auto delayInCopyChunk = FromProto<TDuration>(RemoteCopyJobSpecExt_.delay_in_copy_chunk());
            if (delayInCopyChunk > TDuration::Zero()) {
                YT_LOG_INFO("Sleeping in CopyChunk (DelayInCopyChunk: %v)",
                    delayInCopyChunk);
                Sleep(delayInCopyChunk);
            }
        }

        auto inputChunkId = FromProto<TChunkId>(inputChunkSpec.chunk_id());

        YT_LOG_INFO("Copying chunk (InputChunkId: %v, OutputChunkId: %v)",
            inputChunkId,
            outputSessionId);

        auto erasureCodecId = FromProto<NErasure::ECodec>(inputChunkSpec.erasure_codec());
        if (erasureCodecId != NErasure::ECodec::None) {
            CopyErasureChunk(inputChunkSpec, outputSessionId);
        } else {
            CopyRegularChunk(inputChunkSpec, outputSessionId);
        }
    }

    void DoFinishCopyChunk(const TDeferredChunkMetaPtr& chunkMeta, i64 totalChunkSize)
    {
        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());

        // NB. Compressed data size is already updated for each block in DoCopy, so skip it here.
        DataStatistics_.set_chunk_count(DataStatistics_.chunk_count() + 1);
        DataStatistics_.set_uncompressed_data_size(
            DataStatistics_.uncompressed_data_size() + miscExt.uncompressed_data_size());
        DataStatistics_.set_row_count(DataStatistics_.row_count() + miscExt.row_count());
        DataStatistics_.set_data_weight(DataStatistics_.data_weight() + miscExt.data_weight());

        TotalSize_ -= totalChunkSize;
        CopiedChunkCount_ += 1;
    }

    void CopyErasureChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        auto cancelableContext = New<TCancelableContext>();
        auto suspendableInvoker = CreateSuspendableInvoker(GetRemoteCopyInvoker());
        auto cancelableInvoker = cancelableContext->CreateInvoker(suspendableInvoker);

        auto inputChunkId = FromProto<TChunkId>(inputChunkSpec.chunk_id());
        auto erasureCodecId = FromProto<NErasure::ECodec>(inputChunkSpec.erasure_codec());
        auto erasureCodec = NErasure::GetCodec(erasureCodecId);
        auto inputReplicas = GetReplicasFromChunkSpec(inputChunkSpec);

        auto repairChunk = RemoteCopyJobSpecExt_.repair_erasure_chunks();

        bool isStriped = FromProto<bool>(inputChunkSpec.striped_erasure());

        auto unavailablePartPolicy = repairChunk
            ? EUnavailablePartPolicy::CreateNullReader
            : EUnavailablePartPolicy::Crash;

        YT_VERIFY(!inputChunkSpec.use_proxying_data_node_service());

        auto readers = CreateAllErasurePartReaders(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            MakeChunkReaderHost(),
            inputChunkId,
            std::move(inputReplicas),
            erasureCodec,
            unavailablePartPolicy);

        auto chunkMeta = GetChunkMeta(inputChunkId, readers);
        ReplaceHunkChunkIds(chunkMeta);
        CheckNoCompressionDictionaries(chunkMeta, inputChunkId);

        // We do not support node reallocation for erasure chunks.
        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;

        auto targetReplicas = AllocateWriteTargets(
            Host_->GetClient(),
            outputSessionId,
            erasureCodec->GetTotalPartCount(),
            erasureCodec->GetTotalPartCount(),
            /*replicationFactorOverride*/ std::nullopt,
            /*preferredHostName*/ std::nullopt,
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger());

        auto writers = CreateAllErasurePartWriters(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            erasureCodec,
            Host_->GetClient(),
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler(),
            /*blockCache*/ GetNullBlockCache(),
            targetReplicas);
        YT_VERIFY(readers.size() == writers.size());

        auto stripedErasurePlacementExt = FindProtoExtension<TStripedErasurePlacementExt>(chunkMeta->extensions());
        auto erasurePlacementExt = FindProtoExtension<TErasurePlacementExt>(chunkMeta->extensions());

        i64 totalChunkSize;
        if (isStriped) {
            totalChunkSize = CalculateStripedErasureChunkTotalSize(*stripedErasurePlacementExt, chunkMeta, erasureCodec);
        } else {
            totalChunkSize = CalculateErasureChunkTotalSize(*erasurePlacementExt, chunkMeta);
        }

        TotalSize_ += totalChunkSize;

        std::vector<TFuture<void>> copyFutures;
        copyFutures.reserve(readers.size());
        for (int index = 0; index < std::ssize(readers); ++index) {
            const auto& reader = readers[index];
            const auto& writer = writers[index];

            std::vector<i64> blockSizes;
            if (isStriped) {
                blockSizes = GetStripedErasureChunkPartBlockSizes(*stripedErasurePlacementExt, index);
            } else {
                blockSizes = GetErasureChunkPartBlockSizes(*erasurePlacementExt, erasureCodec, index);
            }

            auto attachDebugAttributes = [inputChunkId, index] (const TError& error) {
                if (!error.IsOK()) {
                    return error
                        << TErrorAttribute("chunk_id", inputChunkId)
                        << TErrorAttribute("part_index", index);
                }
                return error;
            };

            if (reader) {
                auto copyFuture = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
                    .AsyncVia(cancelableInvoker)
                    .Run(reader, writer, blockSizes);
                copyFutures.push_back(copyFuture.Apply(BIND(attachDebugAttributes)));
            } else {
                auto error = attachDebugAttributes(TError("Erasure part is not available"));
                copyFutures.push_back(MakeFuture<void>(error));
            }
        }

        YT_LOG_INFO("Waiting for erasure parts data to be copied (RepairChunk: %v, ChunkId: %v)",
            repairChunk,
            inputChunkId);

        TPartIndexList erasedPartIndices;

        if (repairChunk) {
            auto copyStarted = TInstant::Now();

            auto waitError = WaitUntilErasureChunkCanBeRepaired(erasureCodec, &copyFutures, inputChunkId);
            if (!waitError.IsOK()) {
                FailedChunkIds_.push_back(inputChunkId);
            }
            waitError.ThrowOnError();

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

            TCurrentTraceContextGuard guard(OutputTraceContext_);

            std::vector<TFuture<void>> closeReplicaWriterResults;
            for (int partIndex = 0; partIndex < std::ssize(copyFutures); ++partIndex) {
                auto copyResult = copyResults[partIndex];
                const auto& writer = writers[partIndex];
                if (!copyResult.IsOK()) {
                    erasedPartIndices.push_back(partIndex);
                    closeReplicaWriterResults.push_back(
                        writer
                            ->Cancel()
                            .Apply(
                                BIND([inputChunkId, partIndex] (const TError& error) {
                                    // NB(coteeq): Do not treat error as a failure. Cancellation is
                                    // a hint here, so error is not fatal. We just forget and move on.
                                    YT_LOG_INFO_UNLESS(
                                        error.IsOK(),
                                        error,
                                        "Failed to cancel writer (ChunkId: %v, PartIndex: %v)",
                                        inputChunkId,
                                        partIndex);
                                })));
                } else {
                    closeReplicaWriterResults.push_back(writer->Close(
                        WriteBlocksOptions_,
                        ReaderConfig_->WorkloadDescriptor,
                        chunkMeta));
                }
            }

            WaitFor(AllSucceeded(closeReplicaWriterResults))
                .ThrowOnError();
        } else {
            WaitFor(AllSucceeded(copyFutures))
                .ThrowOnError();

            TCurrentTraceContextGuard guard(OutputTraceContext_);

            std::vector<TFuture<void>> closeReplicaWriterResults;
            closeReplicaWriterResults.reserve(writers.size());
            for (const auto& writer : writers) {
                closeReplicaWriterResults.push_back(writer->Close(
                    WriteBlocksOptions_,
                    ReaderConfig_->WorkloadDescriptor,
                    chunkMeta));
            }
            WaitFor(AllSucceeded(closeReplicaWriterResults))
                .ThrowOnError();
        }

        if (!erasedPartIndices.empty()) {
            if (!isStriped) {
                YT_VERIFY(!inputChunkSpec.use_proxying_data_node_service());

                RepairErasureChunk(
                    outputSessionId,
                    erasureCodec,
                    erasedPartIndices,
                    &writers,
                    targetReplicas);
            } else {
                YT_LOG_INFO("Not all parts were copied successfully, "
                    "chunk should be repaired on the destination cluster "
                    "(ErasedPartCount: %v, InputChunkId: %v, OutputChunkId: %v)",
                    std::ssize(erasedPartIndices),
                    inputChunkId,
                    outputSessionId.ChunkId);
            }
        } else {
            YT_LOG_INFO("All parts were copied successfully (ChunkId: %v)", inputChunkId);
        }

        ChunkFinalizationResults_.push_back(BIND(&TRemoteCopyJob::FinalizeErasureChunk, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(writers, chunkMeta, outputSessionId, inputChunkId, {erasedPartIndices.begin(), erasedPartIndices.end()}));

        DoFinishCopyChunk(chunkMeta, totalChunkSize);
    }

    i64 CalculateStripedErasureChunkTotalSize(
        const TStripedErasurePlacementExt& placementExt,
        const TDeferredChunkMetaPtr& chunkMeta,
        ICodec* erasureCodec)
    {
        i64 parityPartsSize = 0;
        auto firstParityBlockIndex = erasureCodec->GetDataPartCount();
        for (auto index = firstParityBlockIndex; index < placementExt.part_infos_size(); ++index) {
            for (auto size : placementExt.get_idx_part_infos(index).segment_sizes()) {
                parityPartsSize += size;
            }
        }

        // Compute an upper bound for total size.
        return GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size() + parityPartsSize;
    }

    i64 CalculateErasureChunkTotalSize(const TErasurePlacementExt& placementExt, const TDeferredChunkMetaPtr& chunkMeta)
    {
        int parityPartBlockCount = 0;
        for (int count : placementExt.parity_block_count_per_stripe()) {
            parityPartBlockCount += count;
        }

        // Compute an upper bound for total size.
        return GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size() +
            parityPartBlockCount * placementExt.parity_block_size() * placementExt.parity_part_count();
    }

    std::vector<i64> GetStripedErasureChunkPartBlockSizes(
        const TStripedErasurePlacementExt& placementExt,
        i64 partIndex)
    {
        std::vector<i64> blockSizes;
        YT_VERIFY(placementExt.part_infos(partIndex).segment_sizes_size() == placementExt.segment_block_counts_size());
        for (auto segmentSize : placementExt.part_infos(partIndex).segment_sizes()) {
            blockSizes.push_back(segmentSize);
        }
        return blockSizes;
    }

    std::vector<i64> GetErasureChunkPartBlockSizes(
        const TErasurePlacementExt& placementExt,
        ICodec* erasureCodec,
        i64 partIndex)
    {
        std::vector<i64> blockSizes;
        if (partIndex < erasureCodec->GetDataPartCount()) {
            int blockCount = placementExt.part_infos(partIndex).block_sizes_size();
            for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
                blockSizes.push_back(
                    placementExt.part_infos(partIndex).block_sizes(blockIndex));
            }
        } else {
            for (int stripeIndex = 0; stripeIndex < placementExt.parity_block_count_per_stripe_size(); stripeIndex++) {
                blockSizes.insert(
                    blockSizes.end(),
                    placementExt.parity_block_count_per_stripe(stripeIndex),
                    placementExt.parity_block_size());
                blockSizes.back() = placementExt.parity_last_block_size_per_stripe(stripeIndex);
            }
        }
        return blockSizes;
    }

    //! Waits until enough parts were copied to perform repair.
    TError WaitUntilErasureChunkCanBeRepaired(
        NErasure::ICodec* erasureCodec,
        std::vector<TFuture<void>>* copyFutures,
        TChunkId inputChunkId)
    {
        struct TContext final
        {
            // This promise is set when repair can be started.
            TPromise<void> CanStartRepair = NewPromise<void>();

            // Set of parts that were not copied yet.
            TPartIndexSet ErasedPartSet;

            // Set of parts that were copied unsuccessfully.
            TPartIndexSet FailedPartSet;

            std::vector<TError> CopyErrors;
        };
        auto callbackContext = New<TContext>();

        for (int partIndex = 0; partIndex < std::ssize(*copyFutures); ++partIndex) {
            callbackContext->ErasedPartSet.set(partIndex);

            auto& copyFuture = (*copyFutures)[partIndex];
            copyFuture.Subscribe(
                BIND([callbackContext, erasureCodec, partIndex, inputChunkId] (TError error) {
                    if (error.IsOK()) {
                        callbackContext->ErasedPartSet.reset(partIndex);
                        if (erasureCodec->CanRepair(callbackContext->ErasedPartSet)) {
                            callbackContext->CanStartRepair.TrySet();
                        }
                    } else {
                        callbackContext->FailedPartSet.set(partIndex);
                        callbackContext->CopyErrors.push_back(error);
                        // Chunk cannot be repaired, this situation is unrecoverable.
                        if (!erasureCodec->CanRepair(callbackContext->FailedPartSet)) {
                            callbackContext->CanStartRepair.TrySet(TError("Cannot repair erasure chunk")
                                << callbackContext->CopyErrors
                                << TErrorAttribute("chunk_id", inputChunkId));
                        }
                    }
                })
                .Via(GetRemoteCopyInvoker()));
        }

        return WaitFor(callbackContext->CanStartRepair.ToFuture());
    }

    void RepairErasureChunk(
        NChunkClient::TSessionId outputSessionId,
        NErasure::ICodec* erasureCodec,
        const TPartIndexList& erasedPartIndices,
        std::vector<IChunkWriterPtr>* partWriters,
        const TChunkReplicaWithMediumList& targetReplicas)
    {
        TCurrentTraceContextGuard guard(OutputTraceContext_);

        auto repairPartIndices = *erasureCodec->GetRepairIndices(erasedPartIndices);

        YT_LOG_INFO("Failed to copy some of the chunk parts, starting repair "
            "(OutputChunkId: %v, ErasedPartIndices: %v, RepairPartIndices: %v)",
            outputSessionId.ChunkId,
            erasedPartIndices,
            repairPartIndices);

        TChunkReplicaWithMediumList repairSeedReplicas;
        repairSeedReplicas.reserve(repairPartIndices.size());
        for (auto repairPartIndex : repairPartIndices) {
            auto writtenReplicas = (*partWriters)[repairPartIndex]->GetWrittenChunkReplicasInfo().Replicas;
            YT_VERIFY(writtenReplicas.size() == 1);
            auto writtenReplica = writtenReplicas.front();
            repairSeedReplicas.emplace_back(writtenReplica.GetNodeId(), repairPartIndex, writtenReplica.GetMediumIndex());
        }

        TChunkReplicaWithMediumList erasedTargetReplicas;
        erasedTargetReplicas.reserve(erasedPartIndices.size());
        for (auto erasedPartIndex : erasedPartIndices) {
            erasedTargetReplicas.push_back(targetReplicas[erasedPartIndex]);
        }

        auto repairPartReaders = CreateErasurePartReaders(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            Host_->GetChunkReaderHost(),
            outputSessionId.ChunkId,
            std::move(repairSeedReplicas),
            repairPartIndices,
            EUnavailablePartPolicy::Crash);
        YT_VERIFY(repairPartReaders.size() == repairPartIndices.size());

        auto erasedPartWriters = CreateErasurePartWriters(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            Host_->GetClient(),
            erasedPartIndices,
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler(),
            /*blockCache*/ GetNullBlockCache(),
            erasedTargetReplicas);
        YT_VERIFY(erasedPartWriters.size() == erasedPartIndices.size());

        // TODO(yuryalekseev): Calculate estimated size
        ReadBlocksOptions_.EstimatedSize = std::nullopt;

        WaitFor(RepairErasedParts(
            erasureCodec,
            erasedPartIndices,
            std::move(repairPartReaders),
            erasedPartWriters,
            ReadBlocksOptions_,
            WriteBlocksOptions_))
            .ThrowOnError();

        for (int index = 0; index < std::ssize(erasedPartIndices); ++index) {
            (*partWriters)[erasedPartIndices[index]] = erasedPartWriters[index];
        }
    }

    void FinalizeErasureChunk(
        const std::vector<IChunkWriterPtr>& writers,
        const TDeferredChunkMetaPtr& chunkMeta,
        NChunkClient::TSessionId outputSessionId,
        TChunkId inputChunkId,
        THashSet<int> erasedPartIndices)
    {
        TChunkInfo chunkInfo;
        TChunkReplicaWithLocationList writtenReplicas;

        i64 diskSpace = 0;
        for (int index = 0; index < std::ssize(writers); ++index) {
            diskSpace += writers[index]->GetChunkInfo().disk_space();
            auto replicas = writers[index]->GetWrittenChunkReplicasInfo().Replicas;
            if (replicas.empty()) {
                YT_VERIFY(erasedPartIndices.contains(index));
                continue;
            }
            YT_VERIFY(replicas.size() == 1);

            writtenReplicas.emplace_back(
                replicas.front().GetNodeId(),
                index,
                replicas.front().GetMediumIndex(),
                replicas.front().GetChunkLocationUuid());
        }

        chunkInfo.set_disk_space(diskSpace);

        ConfirmChunkReplicas(outputSessionId, chunkInfo, writtenReplicas, chunkMeta, inputChunkId);
    }

    void CopyRegularChunk(const TChunkSpec& inputChunkSpec, NChunkClient::TSessionId outputSessionId)
    {
        auto inputChunkId = FromProto<TChunkId>(inputChunkSpec.chunk_id());
        auto inputReplicas = GetReplicasFromChunkSpec(inputChunkSpec);

        TDeferredChunkMetaPtr chunkMeta;

        YT_VERIFY(!inputChunkSpec.use_proxying_data_node_service());

        auto reader = CreateReplicationReader(
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            MakeChunkReaderHost(),
            inputChunkId,
            std::move(inputReplicas));

        chunkMeta = GetChunkMeta(inputChunkId, {reader});
        ReplaceHunkChunkIds(chunkMeta);
        CheckNoCompressionDictionaries(chunkMeta, inputChunkId);

        auto writer = CreateReplicationWriter(
            WriterConfig_,
            New<TRemoteWriterOptions>(),
            outputSessionId,
            TChunkReplicaWithMediumList(),
            Host_->GetClient(),
            Host_->GetLocalHostName(),
            Host_->GetWriterBlockCache(),
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler());

        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta->extensions());

        std::vector<i64> blockSizes;
        for (const auto& block : blocksExt.blocks()) {
            blockSizes.push_back(block.size());
        }

        i64 totalChunkSize = GetProtoExtension<TMiscExt>(chunkMeta->extensions()).compressed_data_size();

        auto result = BIND(&TRemoteCopyJob::DoCopy, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(reader, writer, blockSizes);

        YT_LOG_INFO("Waiting for chunk data to be copied (ChunkId: %v)", inputChunkId);

        WaitFor(result)
            .ThrowOnError();

        ChunkFinalizationResults_.push_back(BIND(&TRemoteCopyJob::FinalizeRegularChunk, MakeStrong(this))
            .AsyncVia(GetRemoteCopyInvoker())
            .Run(writer, chunkMeta, outputSessionId, inputChunkId));

        DoFinishCopyChunk(chunkMeta, totalChunkSize);
    }

    void FinalizeRegularChunk(
        const IChunkWriterPtr& writer,
        const TDeferredChunkMetaPtr& chunkMeta,
        NChunkClient::TSessionId outputSessionId,
        TChunkId inputChunkId)
    {
        TCurrentTraceContextGuard guard(OutputTraceContext_);

        WaitFor(writer->Close(WriteBlocksOptions_, ReaderConfig_->WorkloadDescriptor, chunkMeta))
            .ThrowOnError();
        TChunkInfo chunkInfo = writer->GetChunkInfo();
        auto writtenReplicas = writer->GetWrittenChunkReplicasInfo().Replicas;
        ConfirmChunkReplicas(outputSessionId, chunkInfo, writtenReplicas, chunkMeta, inputChunkId);
    }

    void ConfirmChunkReplicas(
        NChunkClient::TSessionId outputSessionId,
        const TChunkInfo& chunkInfo,
        const TChunkReplicaWithLocationList& writtenReplicas,
        const TRefCountedChunkMetaPtr& inputChunkMeta,
        TChunkId inputChunkId)
    {
        YT_LOG_INFO("Confirming output chunk (InputChunkId: %v, OutputChunkId: %v)",
            inputChunkId,
            outputSessionId.ChunkId);

        static const THashSet<int> masterMetaTags {
            TProtoExtensionTag<TMiscExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::THunkChunkRefsExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::THunkChunkMiscExt>::Value,
        };

        YT_VERIFY(!writtenReplicas.empty());

        NChunkClient::NProto::TChunkMeta masterChunkMeta(*inputChunkMeta);
        FilterProtoExtensions(
            masterChunkMeta.mutable_extensions(),
            inputChunkMeta->extensions(),
            masterMetaTags);

        TChunkServiceProxy proxy(MasterChannel_);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);

        ToProto(req->mutable_chunk_id(), outputSessionId.ChunkId);
        *req->mutable_chunk_info() = chunkInfo;
        *req->mutable_chunk_meta() = masterChunkMeta;
        ToProto(req->mutable_legacy_replicas(), writtenReplicas);

        req->set_location_uuids_supported(true);

        auto* multicellSyncExt = req->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        multicellSyncExt->set_suppress_upstream_sync(true);

        bool useLocationUuids = std::all_of(writtenReplicas.begin(), writtenReplicas.end(), [] (const TChunkReplicaWithLocation& replica) {
            return replica.GetChunkLocationUuid() != InvalidChunkLocationUuid;
        });

        if (useLocationUuids) {
            for (const auto& replica : writtenReplicas) {
                auto* replicaInfo = req->add_replicas();
                replicaInfo->set_replica(ToProto(TChunkReplicaWithMedium(replica)));
                ToProto(replicaInfo->mutable_location_uuid(), replica.GetChunkLocationUuid());
            }
        }

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            outputSessionId.ChunkId);

        if (IsTableDynamic()) {
            TChunkSpec chunkSpec;
            *chunkSpec.mutable_chunk_meta() = masterChunkMeta;
            ToProto(chunkSpec.mutable_chunk_id(), outputSessionId.ChunkId);
            WrittenChunks_.push_back(chunkSpec);
            if (FromProto<EChunkFormat>(masterChunkMeta.format()) == EChunkFormat::HunkDefault) {
                EmplaceOrCrash(ResultHunkChunkIdMapping_, inputChunkId, outputSessionId.ChunkId);
            }
        }
    }

    void DoCopy(
        IChunkReaderPtr reader,
        IChunkWriterPtr writer,
        const std::vector<i64>& blockSizes)
    {
        TCurrentTraceContextGuard guard(InputTraceContext_);

        auto acquireSemaphoreGuard = [&] {
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

        {
            TCurrentTraceContextGuard guard(OutputTraceContext_);

            auto error = WaitFor(writer->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error opening writer");
        }

        int blockCount = std::ssize(blockSizes);

        int blockIndex = 0;
        while (blockIndex < blockCount) {
            int beginBlockIndex = blockIndex;
            int endBlockIndex = blockIndex;
            i64 sizeToRead = 0;

            while (endBlockIndex < blockCount && sizeToRead + blockSizes[endBlockIndex] <= RemoteCopyJobSpecExt_.block_buffer_size()) {
                sizeToRead += blockSizes[endBlockIndex];
                endBlockIndex += 1;
            }

            // This can happen if we encounter block which is bigger than block buffer size.
            // In this case at least one block should be read (this memory overhead is taken
            // into account in operation controller).
            if (endBlockIndex == beginBlockIndex) {
                sizeToRead += blockSizes[endBlockIndex];
                endBlockIndex += 1;
            }

            if (JobSpecExt_.use_cluster_throttlers()) {
                ReadBlocksOptions_.EstimatedSize = sizeToRead;
            }

            std::vector<int> blockIndices(endBlockIndex - beginBlockIndex);
            std::iota(blockIndices.begin(), blockIndices.end(), beginBlockIndex);
            auto asyncResult = reader->ReadBlocks(
                ReadBlocksOptions_,
                blockIndices);

            auto result = WaitFor(asyncResult);
            if (!result.IsOK()) {
                FailedChunkIds_.push_back(reader->GetChunkId());
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reading blocks");
            }

            const auto& blocks = result.Value();

            i64 blocksSize = GetByteSize(blocks);
            CopiedSize_ += blocksSize;

            {
                TCurrentTraceContextGuard guard(OutputTraceContext_);

                if (!writer->WriteBlocks(WriteBlocksOptions_, ReaderConfig_->WorkloadDescriptor, blocks)) {
                    auto result = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error writing block");
                }
            }

            DataStatistics_.set_compressed_data_size(DataStatistics_.compressed_data_size() + blocksSize);

            blockIndex += blocks.size();
        }
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    TDeferredChunkMetaPtr GetChunkMeta(
        TChunkId chunkId,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers)
    {
        TCurrentTraceContextGuard guard(InputTraceContext_);

        // In erasure chunks some of the parts might be unavailable, but they all have the same meta,
        // so we try to get meta from all of the readers simultaneously.
        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncResults;
        asyncResults.reserve(readers.size());
        for (const auto& reader : readers) {
            if (reader) {
                asyncResults.push_back(reader->GetMeta(IChunkReader::TGetMetaOptions{
                    .ClientOptions = ReadBlocksOptions_.ClientOptions,
                }));
            }
        }

        auto result = WaitFor(AnySucceeded(asyncResults));
        if (!result.IsOK()) {
            FailedChunkIds_.push_back(chunkId);
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to get chunk meta");
        }

        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->CopyFrom(*result.Value());
        return deferredChunkMeta;
    }

    bool IsTableDynamic() const
    {
        return JobSpecExt_.output_table_specs(0).dynamic();
    }

    TChunkReaderHostPtr MakeChunkReaderHost()
    {
        YT_LOG_DEBUG("Creating chunk reader host (RemoteClusterName: %v, UseClusterThrottlers: %v)",
            RemoteCopyJobSpecExt_.remote_cluster_name(),
            JobSpecExt_.use_cluster_throttlers());

        auto clusterName = LocalClusterName;
        if (RemoteCopyJobSpecExt_.has_remote_cluster_name() && JobSpecExt_.use_cluster_throttlers()) {
            clusterName = FromProto<TClusterName>(RemoteCopyJobSpecExt_.remote_cluster_name());
        }

        auto bandwidthThrottlerFactory = BIND([this, weakThis = MakeWeak(this)] (const TClusterName& clusterName) {
            auto thisLocked = weakThis.Lock();
            if (!thisLocked) {
                return IThroughputThrottlerPtr();
            }

            return Host_->GetInBandwidthThrottler(clusterName);
        });

        return New<TChunkReaderHost>(
            RemoteClient_,
            Host_->LocalDescriptor(),
            Host_->GetReaderBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            bandwidthThrottlerFactory(clusterName),
            Host_->GetOutRpsThrottler(),
            /*mediumThrottler*/ GetUnlimitedThrottler(),
            Host_->GetTrafficMeter(),
            std::move(bandwidthThrottlerFactory));
    }

    void ReplaceHunkChunkIds(const TDeferredChunkMetaPtr& chunkMeta)
    {
        if (HunkChunkIdMapping_.empty()) {
            return;
        }

        auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
            chunkMeta->extensions());
        auto optionalHunkChunkMetasExt = FindProtoExtension<NTableClient::NProto::THunkChunkMetasExt>(
            chunkMeta->extensions());

        YT_VERIFY(optionalHunkChunkRefsExt.has_value() == optionalHunkChunkMetasExt.has_value());
        if (!optionalHunkChunkRefsExt) {
            return;
        }

        for (auto& ref : *optionalHunkChunkRefsExt->mutable_refs()) {
            auto chunkId = FromProto<TChunkId>(ref.chunk_id());
            ToProto(ref.mutable_chunk_id(), GetOrCrash(HunkChunkIdMapping_, chunkId));
        }

        for (auto& ref : *optionalHunkChunkMetasExt->mutable_metas()) {
            auto chunkId = FromProto<TChunkId>(ref.chunk_id());
            ToProto(ref.mutable_chunk_id(), GetOrCrash(HunkChunkIdMapping_, chunkId));
        }

        SetProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
            chunkMeta->mutable_extensions(),
            *optionalHunkChunkRefsExt);
        SetProtoExtension<NTableClient::NProto::THunkChunkMetasExt>(
            chunkMeta->mutable_extensions(),
            *optionalHunkChunkMetasExt);
    }

    void CheckNoCompressionDictionaries(const TDeferredChunkMetaPtr& chunkMeta, TChunkId chunkId)
    {
        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
        if (miscExt.has_compression_dictionary_id()) {
            THROW_ERROR_EXCEPTION("Compression dictionaries are not supported for this type of jobs")
                << TErrorAttribute("chunk_id", chunkId)
                << TErrorAttribute("job_type", EJobType::RemoteCopy);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateRemoteCopyJob(IJobHostPtr host)
{
    return New<TRemoteCopyJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
