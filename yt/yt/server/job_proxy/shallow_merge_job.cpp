#include "shallow_merge_job.h"

#include "private.h"
#include "job_detail.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/meta_aggregating_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TShallowMergeJob
    : public TJob
{
public:
    explicit TShallowMergeJob(IJobHost* host)
        : TJob(host)
        , SchedulerJobSpecExt_(host->GetJobSpecHelper()->GetSchedulerJobSpecExt())
        , ShallowMergeJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TShallowMergeJobSpecExt::shallow_merge_job_spec_ext))
        , ReaderConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader)
        , WriterConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter)
    {
        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);
    }

    void Initialize() override
    {
        YT_LOG_INFO("Shallow merge job initializing");

        ChunkReadOptions_.WorkloadDescriptor = ReaderConfig_->WorkloadDescriptor;
        ChunkReadOptions_.ChunkReaderStatistics = New<NChunkClient::TChunkReaderStatistics>();
        ChunkReadOptions_.ReadSessionId = TReadSessionId::Create();

        ReaderOptions_ = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpecExt_.table_reader_options()));

        CreateChunkWriters();
        BuildInputChunkStates();
        CalculateTotalBlocksSize();

        YT_LOG_INFO("Shallow merge job initialized");
    }

    TJobResult Run() override
    {
        Host_->OnPrepared();

        if (Host_->GetJobSpecHelper()->GetJobTestingOptions()->ThrowInShallowMerge) {
            THROW_ERROR_EXCEPTION(EErrorCode::ShallowMergeFailed, "Shallow merge is aborted");
        }

        YT_LOG_INFO("Shallow merge is running");
        YT_LOG_DEBUG("Absorbing metas");

        try {
            AbsorbMetas();
        } catch (const std::exception& ex) {
           YT_LOG_INFO(TError(ex), "Error absorbing metas");
           THROW_ERROR_EXCEPTION(EErrorCode::ShallowMergeFailed, "Shallow merge failed") << ex;
        }

        YT_LOG_DEBUG("Shallow merging blocks");
        MergeInputChunks();

        TJobResult jobResult;
        ToProto(jobResult.mutable_error(), TError());
        auto* schedulerResultExt = jobResult.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        ToProto(schedulerResultExt->add_output_chunk_specs(), GetOutputChunkSpec());

        YT_LOG_INFO("Shallow merge completed");
        return jobResult;
    }

    void Cleanup() override
    { }

    void PrepareArtifacts() override
    { }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkIds_;
    }

    NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return {};
    }

    void Interrupt() override
    {
        THROW_ERROR_EXCEPTION("Interrupting is not supported for this type of jobs")
            << TErrorAttribute("job_type", NJobTrackerClient::EJobType::ShallowMerge);
    }

    double GetProgress() const override
    {
        YT_VERIFY(TotalBlocksSize_ != 0);
        return static_cast<double>(ProcessedBlocksSize_) / TotalBlocksSize_;
    }

    TStatistics GetStatistics() const override
    {
        TStatistics result;
        result.AddSample("/data/input", InputDataStatistics_);
        result.AddSample("/data/output/" + NYPath::ToYPathLiteral(0), OutputDataStatistics_);
        return result;
    }

private:
    struct TInputChunkState
    {
        IChunkReaderPtr Reader;
        TDeferredChunkMetaPtr Meta;
        TChunkId ChunkId;
        i64 CompressedDataSize;
        i64 UncompressedDataSize;
        i64 DataWeight;
        i64 RowCount;
        int BlockCount;
        bool IsErasureChunk;
    };

    const TSchedulerJobSpecExt SchedulerJobSpecExt_;
    const TShallowMergeJobSpecExt ShallowMergeJobSpecExt_;

    const TTableReaderConfigPtr ReaderConfig_;
    const TTableWriterConfigPtr WriterConfig_;

    TTableReaderOptionsPtr ReaderOptions_;
    TTableWriterOptionsPtr UnderlyingWriterOptions_;

    IMetaAggregatingWriterPtr Writer_;

    TClientChunkReadOptions ChunkReadOptions_;

    std::vector<TInputChunkState> InputChunkStates_;

    i64 TotalBlocksSize_ = 0;
    i64 ProcessedBlocksSize_ = 0;

    std::vector<TChunkId> FailedChunkIds_;
    TDataStatistics InputDataStatistics_;
    TDataStatistics OutputDataStatistics_;

    void CreateChunkWriters()
    {
        const auto& outputTableSpec = SchedulerJobSpecExt_.output_table_specs(0);
        auto outputChunkListId = FromProto<TChunkListId>(outputTableSpec.chunk_list_id());

        UnderlyingWriterOptions_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(outputTableSpec.table_writer_options()));

        auto writerOptions = New<TMetaAggregatingWriterOptions>();
        DeserializeFromWireProto(&writerOptions->TableSchema, outputTableSpec.table_schema());
        writerOptions->CompressionCodec = UnderlyingWriterOptions_->CompressionCodec;
        writerOptions->ErasureCodec = UnderlyingWriterOptions_->ErasureCodec;
        writerOptions->EnableSkynetSharing = UnderlyingWriterOptions_->EnableSkynetSharing;
        writerOptions->MaxHeavyColumns = UnderlyingWriterOptions_->MaxHeavyColumns;
        writerOptions->AllowUnknownExtensions = ShallowMergeJobSpecExt_.allow_unknown_extensions();

        auto underlyingWriter = CreateConfirmingWriter(
            /*config*/ WriterConfig_,
            /*options*/ UnderlyingWriterOptions_,
            /*cellTag*/ CellTagFromId(outputChunkListId),
            /*transactionId*/ FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id()),
            /*parentChunkListId*/ outputChunkListId,
            /*nodeDirectory*/ New<TNodeDirectory>(),
            /*client*/ Host_->GetClient(),
            /*blockCache*/ Host_->GetWriterBlockCache(),
            /*trafficMeter*/ Host_->GetTrafficMeter(),
            /*throttler*/ Host_->GetOutBandwidthThrottler());
        Writer_ = CreateMetaAggregatingWriter(std::move(underlyingWriter), writerOptions);
    }

    IChunkReaderPtr CreateChunkReader(const TChunkSpec& chunk) const
    {
        return CreateRemoteReader(
            /*chunkSpec*/ chunk,
            /*config*/ ReaderConfig_,
            /*options*/ ReaderOptions_,
            /*client*/ Host_->GetClient(),
            /*nodeDirectory*/ Host_->GetInputNodeDirectory(),
            /*localDescriptor*/ Host_->LocalDescriptor(),
            /*localNodeId*/ std::nullopt,
            /*blockCache*/ Host_->GetReaderBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*trafficMeter*/ Host_->GetTrafficMeter(),
            /*nodeStatusDirectory*/ nullptr,
            /*bandwidthThrottler*/ Host_->GetInBandwidthThrottler(),
            /*rpsThrottler*/ Host_->GetOutRpsThrottler());
    }

    TDeferredChunkMetaPtr GetChunkMeta(const IChunkReaderPtr& reader)
    {
        auto result = WaitFor(reader->GetMeta(ChunkReadOptions_));
        if (!result.IsOK()) {
            FailedChunkIds_.push_back(reader->GetChunkId());
            THROW_ERROR_EXCEPTION("Failed to get chunk meta") << result;
        }

        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->CopyFrom(*result.Value());
        return deferredChunkMeta;
    }

    TInputChunkState BuildInputChunkStateFromSpec(const TChunkSpec& chunkSpec)
    {
        YT_LOG_DEBUG("Building chunk state (ChunkId: %v)", chunkSpec.chunk_id());

        try {
            auto reader = CreateChunkReader(chunkSpec);
            auto chunkMeta = GetChunkMeta(reader);
            auto blocksExt = GetProtoExtension<TBlockMetaExt>(chunkMeta->extensions());
            auto erasureCodec = CheckedEnumCast<NErasure::ECodec>(chunkSpec.erasure_codec());
            auto chunkId = reader->GetChunkId();
            auto blockCount = blocksExt.blocks_size();
            bool isErasureChunk = erasureCodec != NErasure::ECodec::None;
            auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());

            return TInputChunkState{
                std::move(reader),
                std::move(chunkMeta),
                chunkId,
                miscExt.compressed_data_size(),
                miscExt.uncompressed_data_size(),
                miscExt.data_weight(),
                miscExt.row_count(),
                blockCount,
                isErasureChunk
            };
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to build chunk state")
                << ex
                << TErrorAttribute("chunk_id", chunkSpec.chunk_id());
        }
    }

    void BuildInputChunkStates()
    {
        std::vector<TFuture<TInputChunkState>> asyncResults;
        for (const auto& tableSpec : SchedulerJobSpecExt_.input_table_specs()) {
            for (const auto& chunkSpec : tableSpec.chunk_specs()) {
                auto asyncResult = BIND(&TShallowMergeJob::BuildInputChunkStateFromSpec, MakeStrong(this), chunkSpec)
                    .AsyncVia(GetCurrentInvoker())
                    .Run();
                asyncResults.push_back(std::move(asyncResult));
            }
        }
        InputChunkStates_ = WaitFor(AllSucceeded(asyncResults))
            .ValueOrThrow();
    }

    void CalculateTotalBlocksSize()
    {
        for (const auto& inputChunkInfo : InputChunkStates_) {
            TotalBlocksSize_ += inputChunkInfo.CompressedDataSize;
        }
    }

    void DoWriteBlocks(const std::vector<TBlock>& blocks)
    {
        if (!Writer_->WriteBlocks(blocks)) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }

        i64 blocksSize = 0;
        for (const auto& block : blocks) {
            blocksSize += block.Size();
        }
        InputDataStatistics_.set_compressed_data_size(InputDataStatistics_.compressed_data_size() + blocksSize);
        OutputDataStatistics_.set_compressed_data_size(OutputDataStatistics_.compressed_data_size() + blocksSize);
        ProcessedBlocksSize_ += blocksSize;
    }

    std::vector<i64> GetBlockSizes(const TInputChunkState& chunkState) const
    {
        auto blocksExt = GetProtoExtension<TBlocksExt>(chunkState.Meta->extensions());
        std::vector<i64> blockSizes;
        blockSizes.reserve(chunkState.BlockCount);
        for (const auto& block : blocksExt.blocks()) {
            blockSizes.push_back(block.size());
        }
        YT_VERIFY(ssize(blockSizes) == chunkState.BlockCount);
        return blockSizes;
    }

    int CalculateBlockCountToRead(
        const TInputChunkState& chunkState,
        const std::vector<i64>& blockSizes,
        int firstBlockIndex)
    {
        int lastBlockIndex = firstBlockIndex;
        i64 sizeToRead = 0;
        i64 bufferSize = ReaderConfig_->WindowSize;

        while (lastBlockIndex < chunkState.BlockCount && sizeToRead + blockSizes[lastBlockIndex] <= bufferSize) {
            sizeToRead += blockSizes[lastBlockIndex];
            ++lastBlockIndex;
        }

        int blockCountToRead = lastBlockIndex - firstBlockIndex;

        // This can happen if we encounter a block which is bigger than the specified buffer size.
        // In this case at least one block should be read.
        if (blockCountToRead == 0) {
            YT_VERIFY(firstBlockIndex < chunkState.BlockCount);
            blockCountToRead = 1;
        }

        return blockCountToRead;
    }

    void MergeInputChunk(const TInputChunkState& chunkState)
    {
        YT_LOG_DEBUG("Merging input chunk (ChunkId: %v)", chunkState.ChunkId);

        auto blockSizes = GetBlockSizes(chunkState);
        int chunkBlockCount = chunkState.BlockCount;
        int currentBlockCount = 0;
        while (currentBlockCount < chunkBlockCount) {
            std::vector<TBlock> blocks;
            try {
                auto asyncBlocks = chunkState.Reader->ReadBlocks(
                    ChunkReadOptions_,
                    currentBlockCount,
                    CalculateBlockCountToRead(chunkState, blockSizes, currentBlockCount));
                blocks = WaitFor(asyncBlocks)
                    .ValueOrThrow();
            } catch (const std::exception& ex) {
                FailedChunkIds_.push_back(chunkState.ChunkId);
                throw;
            }

            YT_VERIFY(!blocks.empty());
            DoWriteBlocks(blocks);
            currentBlockCount += ssize(blocks);
        }

        InputDataStatistics_.set_chunk_count(InputDataStatistics_.chunk_count() + 1);

        // NB. CompressedDataSize is updated in DoWriteBlocks, so don't update it here.

        InputDataStatistics_.set_uncompressed_data_size(
            InputDataStatistics_.uncompressed_data_size() + chunkState.UncompressedDataSize);
        InputDataStatistics_.set_row_count(InputDataStatistics_.row_count() + chunkState.RowCount);
        InputDataStatistics_.set_data_weight(InputDataStatistics_.data_weight() + chunkState.DataWeight);

        OutputDataStatistics_.set_uncompressed_data_size(
            OutputDataStatistics_.uncompressed_data_size() + chunkState.UncompressedDataSize);
        OutputDataStatistics_.set_row_count(OutputDataStatistics_.row_count() + chunkState.RowCount);
        OutputDataStatistics_.set_data_weight(OutputDataStatistics_.data_weight() + chunkState.DataWeight);
    }

    void AbsorbMetas()
    {
        for (const auto& chunkState : InputChunkStates_) {
            // TODO(gepardo): Support shallow merge for erasure chunks (see YT-15343).
            if (chunkState.IsErasureChunk) {
                THROW_ERROR_EXCEPTION("Erasure chunks are not supported by shallow by this moment");
            }

            Writer_->AbsorbMeta(chunkState.Meta, chunkState.ChunkId);
        }
    }

    void MergeInputChunks()
    {
        WaitFor(Writer_->Open())
            .ThrowOnError();
        for (const auto& chunkState : InputChunkStates_) {
            try {
                MergeInputChunk(chunkState);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to process chunk")
                    << ex
                    << TErrorAttribute("chunk_id", chunkState.ChunkId);
            }
        }
        WaitFor(Writer_->Close())
            .ThrowOnError();
        OutputDataStatistics_.set_chunk_count(1);
    }

    TChunkSpec GetOutputChunkSpec() const
    {
        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), Writer_->GetChunkId());
        ToProto(chunkSpec.mutable_replicas(), Writer_->GetWrittenChunkReplicas());
        chunkSpec.set_erasure_codec(ToProto<int>(UnderlyingWriterOptions_->ErasureCodec));
        chunkSpec.set_table_index(0);
        *chunkSpec.mutable_chunk_meta() = std::move(*Writer_->GetChunkMeta());
        return chunkSpec;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateShallowMergeJob(IJobHost *host)
{
    return New<TShallowMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
