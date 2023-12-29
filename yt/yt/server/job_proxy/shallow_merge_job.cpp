#include "shallow_merge_job.h"

#include "private.h"
#include "job_detail.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/meta_aggregating_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/job_proxy/config.h>
#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NControllerAgent::NProto;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NTracing;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

constexpr auto ShallowMergeStatisticsUpdatePeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TShallowMergeJob
    : public TJob
{
public:
    explicit TShallowMergeJob(IJobHostPtr host)
        : TJob(host)
        , JobSpecExt_(host->GetJobSpecHelper()->GetJobSpecExt())
        , ShallowMergeJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TShallowMergeJobSpecExt::shallow_merge_job_spec_ext))
        , ReaderConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader)
        , WriterConfig_(Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter)
        , OutputTraceContext_(NTracing::CreateTraceContextFromCurrent("TableWriter"))
        , OutputFinishGuard_(OutputTraceContext_)
    {
        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);
    }

    void Initialize() override
    {
        TJob::Initialize();

        YT_LOG_INFO("Shallow merge job initializing");

        ChunkReadOptions_.WorkloadDescriptor = ReaderConfig_->WorkloadDescriptor;
        ChunkReadOptions_.ChunkReaderStatistics = New<NChunkClient::TChunkReaderStatistics>();
        ChunkReadOptions_.ReadSessionId = TReadSessionId::Create();

        ReaderOptions_ = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            JobSpecExt_.table_reader_options()));

        PackBaggages();
        CreateChunkWriters();

        YT_LOG_INFO("Shallow merge job initialized");
    }

    TJobResult Run() override
    {
        YT_LOG_INFO("Shallow merge job preparing");

        BuildInputChunkStates();
        CalculateTotalBlocksSize();
        Prepared_.store(true);

        YT_LOG_INFO("Shallow merge job prepared");

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
        {
            auto* jobResultExt = jobResult.MutableExtension(TJobResultExt::job_result_ext);
            ToProto(jobResultExt->add_output_chunk_specs(), GetOutputChunkSpec());
        }

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
        if (!Prepared_.load()) {
            return 0.0;
        }

        YT_VERIFY(TotalBlocksSize_ != 0);
        return static_cast<double>(ProcessedBlocksSize_) / TotalBlocksSize_;
    }

    TStatistics GetStatistics() const override
    {
        return {
            .ChunkReaderStatistics = ChunkReadOptions_.ChunkReaderStatistics,
            .TotalInputStatistics = {
                .DataStatistics = InputDataStatistics_.Load(),
            },
            .OutputStatistics = {
                {
                    .DataStatistics = OutputDataStatistics_.Load(),
                },
            },
        };
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
        int InputSpecIndex;
        int SystemBlockCount;
        bool IsErasureChunk;
    };

    const TJobSpecExt JobSpecExt_;
    const TShallowMergeJobSpecExt ShallowMergeJobSpecExt_;

    const TTableReaderConfigPtr ReaderConfig_;
    const TTableWriterConfigPtr WriterConfig_;

    TTableReaderOptionsPtr ReaderOptions_;
    TTableWriterOptionsPtr UnderlyingWriterOptions_;

    IMetaAggregatingWriterPtr Writer_;

    TClientChunkReadOptions ChunkReadOptions_;

    std::vector<TInputChunkState> InputChunkStates_;

    std::atomic<bool> Prepared_ = false;
    i64 TotalBlocksSize_ = 0;
    i64 ProcessedBlocksSize_ = 0;

    std::vector<TChunkId> FailedChunkIds_;

    TAtomicObject<TDataStatistics> InputDataStatistics_;
    TAtomicObject<TDataStatistics> OutputDataStatistics_;

    std::vector<TTraceContextPtr> InputTraceContexts_;
    std::vector<TTraceContextFinishGuard> InputFinishGuards_;
    TTraceContextPtr OutputTraceContext_;
    TTraceContextFinishGuard OutputFinishGuard_;

    void PackBaggages()
    {
        auto dataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(JobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(*dataSourceDirectoryExt);

        TExtraChunkTags extraChunkTags;

        for (int inputSpecIndex = 0; inputSpecIndex < JobSpecExt_.input_table_specs_size(); ++inputSpecIndex) {
            const auto& tableSpec = JobSpecExt_.input_table_specs()[inputSpecIndex];
            const auto& traceContext = InputTraceContexts_.emplace_back(
                CreateTraceContextFromCurrent(Format("TableReader.%v", inputSpecIndex)));
            InputFinishGuards_.emplace_back(traceContext);
            if (tableSpec.chunk_specs_size() != 0) {
                int tableIndex = tableSpec.chunk_specs()[0].table_index();
                extraChunkTags = TExtraChunkTags{
                    .ErasureCodec = NErasure::ECodec(tableSpec.chunk_specs()[0].erasure_codec()),
                };
                PackBaggageForChunkReader(
                    traceContext,
                    dataSourceDirectory->DataSources()[tableIndex],
                    extraChunkTags);
            }
        }

        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(JobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            PackBaggageForChunkWriter(OutputTraceContext_, dataSinkDirectory->DataSinks()[0], extraChunkTags);
        }
    }

    void CreateChunkWriters()
    {
        const auto& outputTableSpec = JobSpecExt_.output_table_specs(0);
        auto outputChunkListId = FromProto<TChunkListId>(outputTableSpec.chunk_list_id());
        auto schemaId = FromProto<TMasterTableSchemaId>(outputTableSpec.schema_id());

        UnderlyingWriterOptions_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(outputTableSpec.table_writer_options()));

        auto writerOptions = New<TMetaAggregatingWriterOptions>();
        DeserializeFromWireProto(&writerOptions->TableSchema, outputTableSpec.table_schema());
        writerOptions->CompressionCodec = UnderlyingWriterOptions_->CompressionCodec;
        writerOptions->ErasureCodec = UnderlyingWriterOptions_->ErasureCodec;
        writerOptions->EnableSkynetSharing = UnderlyingWriterOptions_->EnableSkynetSharing;
        writerOptions->MaxHeavyColumns = UnderlyingWriterOptions_->MaxHeavyColumns;
        writerOptions->AllowUnknownExtensions = ShallowMergeJobSpecExt_.allow_unknown_extensions();
        if (ShallowMergeJobSpecExt_.has_max_block_count()) {
            writerOptions->MaxBlockCount = ShallowMergeJobSpecExt_.max_block_count();
        }

        auto underlyingWriter = CreateConfirmingWriter(
            WriterConfig_,
            UnderlyingWriterOptions_,
            CellTagFromId(outputChunkListId),
            FromProto<TTransactionId>(JobSpecExt_.output_transaction_id()),
            schemaId,
            outputChunkListId,
            Host_->GetClient(),
            Host_->GetLocalHostName(),
            Host_->GetWriterBlockCache(),
            Host_->GetTrafficMeter(),
            Host_->GetOutBandwidthThrottler());
        Writer_ = CreateMetaAggregatingWriter(std::move(underlyingWriter), writerOptions);
    }

    IChunkReaderPtr CreateChunkReader(const TChunkSpec& chunkSpec) const
    {
        return CreateRemoteReader(
            chunkSpec,
            ReaderConfig_,
            ReaderOptions_,
            Host_->GetChunkReaderHost());
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

    TInputChunkState BuildInputChunkStateFromSpec(const TChunkSpec& chunkSpec, int inputSpecIndex)
    {
        YT_LOG_DEBUG("Building chunk state (ChunkId: %v)", chunkSpec.chunk_id());

        try {
            auto reader = CreateChunkReader(chunkSpec);
            auto chunkMeta = GetChunkMeta(reader);
            auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(chunkMeta->extensions());
            auto blockCount = blockMetaExt.data_blocks_size();
            auto erasureCodec = CheckedEnumCast<NErasure::ECodec>(chunkSpec.erasure_codec());
            auto chunkId = reader->GetChunkId();
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
                inputSpecIndex,
                miscExt.system_block_count(),
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
        for (int inputSpecIndex = 0; inputSpecIndex < JobSpecExt_.input_table_specs_size(); ++inputSpecIndex) {
            const auto& tableSpec = JobSpecExt_.input_table_specs()[inputSpecIndex];

            for (const auto& chunkSpec : tableSpec.chunk_specs()) {
                auto asyncResult = BIND(&TShallowMergeJob::BuildInputChunkStateFromSpec,
                        MakeStrong(this),
                        chunkSpec,
                        inputSpecIndex)
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

    std::vector<i64> GetBlockSizes(const TInputChunkState& chunkState) const
    {
        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkState.Meta->extensions());
        std::vector<i64> blockSizes;
        blockSizes.reserve(chunkState.BlockCount);
        for (const auto& block : blocksExt.blocks()) {
            blockSizes.push_back(block.size());
        }
        YT_VERIFY(ssize(blockSizes) == chunkState.BlockCount);
        return blockSizes;
    }

    void AbsorbMetas()
    {
        for (const auto& chunkState : InputChunkStates_) {
            // TODO(gepardo): Support shallow merge for erasure chunks (see YT-15343).
            if (chunkState.IsErasureChunk) {
                THROW_ERROR_EXCEPTION("Erasure chunks are not supported by shallow merge by this moment");
            }
            // TODO(akozhikhov).
            if (chunkState.SystemBlockCount > 0) {
                THROW_ERROR_EXCEPTION("Chunks containing system blocks cannot be merged");
            }

            Writer_->AbsorbMeta(chunkState.Meta, chunkState.ChunkId);
        }
    }

    void OnChunkReadFinished(int chunkIndex)
    {
        const auto& chunkState = InputChunkStates_[chunkIndex];

        // NB. CompressedDataSize is updated in UpdateCompressedDataSize, so don't update it here.

        InputDataStatistics_.Transform([&] (auto& statistics) {
            statistics.set_chunk_count(statistics.chunk_count() + 1);

            statistics.set_uncompressed_data_size(statistics.uncompressed_data_size() + chunkState.UncompressedDataSize);
            statistics.set_row_count(statistics.row_count() + chunkState.RowCount);
            statistics.set_data_weight(statistics.data_weight() + chunkState.DataWeight);
        });

        OutputDataStatistics_.Transform([&] (auto& statistics) {
            statistics.set_uncompressed_data_size(statistics.uncompressed_data_size() + chunkState.UncompressedDataSize);
            statistics.set_row_count(statistics.row_count() + chunkState.RowCount);
            statistics.set_data_weight(statistics.data_weight() + chunkState.DataWeight);
        });
    }

    i64 CalculateTotalBlockCount() const
    {
        i64 result = 0;
        for (const auto& state : InputChunkStates_) {
            result += state.BlockCount;
        }
        return result;
    }

    void UpdateCompressedDataSize(const TBlockFetcherPtr& blockFetcher)
    {
        InputDataStatistics_.Transform([&] (auto& statistics) {
            statistics.set_compressed_data_size(blockFetcher->GetCompressedDataSize());
        });

        OutputDataStatistics_.Transform([&] (auto& statistics) {
            statistics.set_compressed_data_size(blockFetcher->GetCompressedDataSize());
        });
    }

    TBlockFetcherPtr CreateBlockFetcher()
    {
        // NB(gepardo): The blocks may be compressed, and block fetcher usually performs decompression.
        // However, we want to obtain compressed blocks here as-is. So, we tell the block fetcher that
        // blocks are uncompressed, effectively disabling decompression.

        std::vector<TBlockFetcher::TBlockInfo> blockInfos;
        std::vector<IChunkReaderPtr> chunkReaders;
        chunkReaders.reserve(InputChunkStates_.size());
        blockInfos.reserve(CalculateTotalBlockCount());

        for (int chunkIndex = 0; chunkIndex < std::ssize(InputChunkStates_); ++chunkIndex) {
            auto& state = InputChunkStates_[chunkIndex];
            chunkReaders.push_back(state.Reader);
            auto blockSizes = GetBlockSizes(state);
            for (int blockIndex = 0; blockIndex < state.BlockCount; ++blockIndex) {
                blockInfos.push_back(TBlockFetcher::TBlockInfo{
                    .ReaderIndex = chunkIndex,
                    .BlockIndex = blockIndex,
                    .UncompressedDataSize = blockSizes[blockIndex],
                    .BlockType = EBlockType::None,
                });
            }
        }

        auto memoryManager = New<TChunkReaderMemoryManager>(
            TChunkReaderMemoryManagerOptions(ReaderConfig_->WindowSize));

        auto statisticsInvoker = CreateSerializedInvoker(GetCurrentInvoker(), "shallow_merge_job");

        auto blockFetcher = New<TBlockFetcher>(
            ReaderConfig_,
            std::move(blockInfos),
            std::move(memoryManager),
            std::move(chunkReaders),
            /*blockCache*/ GetNullBlockCache(),
            /*compressionCodec*/ ECodec::None,
            /*compressionRatio*/ 1.0,
            ChunkReadOptions_);

        blockFetcher->SubscribeOnReaderFinished(
            BIND(&TShallowMergeJob::OnChunkReadFinished, MakeWeak(this))
                .Via(statisticsInvoker));
        for (int chunkIndex = 0; chunkIndex < std::ssize(InputChunkStates_); ++chunkIndex) {
            blockFetcher->SetTraceContextForReader(
                chunkIndex,
                InputTraceContexts_[InputChunkStates_[chunkIndex].InputSpecIndex]);
        };

        blockFetcher->Start();
        return blockFetcher;
    }

    void MergeInputChunks()
    {
        TCurrentTraceContextGuard outputGuard(OutputTraceContext_);

        WaitFor(Writer_->Open())
            .ThrowOnError();

        auto blockFetcher = CreateBlockFetcher();
        auto statisticsUpdater = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TShallowMergeJob::UpdateCompressedDataSize, MakeWeak(this), blockFetcher),
            ShallowMergeStatisticsUpdatePeriod);
        auto finally = Finally([&] {
            UpdateCompressedDataSize(blockFetcher);
        });

        for (int chunkIndex = 0; chunkIndex < std::ssize(InputChunkStates_); ++chunkIndex) {
            const auto& chunkState = InputChunkStates_[chunkIndex];
            for (int blockIndex = 0; blockIndex < chunkState.BlockCount; ++blockIndex) {
                try {
                    auto block = WaitForUniqueFast(blockFetcher->FetchBlock(chunkIndex, blockIndex))
                        .ValueOrThrow();

                    if (!Writer_->WriteBlock(ReaderConfig_->WorkloadDescriptor, block)) {
                        WaitFor(Writer_->GetReadyEvent())
                            .ThrowOnError();
                    }
                    ProcessedBlocksSize_ += block.Size();
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to process block")
                        << ex
                        << TErrorAttribute("input_chunk_id", chunkState.ChunkId)
                        << TErrorAttribute("input_block_index", blockIndex);
                }
            }
        }

        WaitFor(Writer_->Close())
            .ThrowOnError();

        OutputDataStatistics_.Transform([] (auto& statistics) {
            statistics.set_chunk_count(1);
        });
    }

    TChunkSpec GetOutputChunkSpec() const
    {
        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), Writer_->GetChunkId());
        auto replicas = Writer_->GetWrittenChunkReplicas();
        for (auto replica : replicas) {
            chunkSpec.add_legacy_replicas(ToProto<ui32>(replica.ToChunkReplica()));
        }
        ToProto(chunkSpec.mutable_replicas(), replicas);
        chunkSpec.set_table_index(0);

        const auto& chunkMeta = *Writer_->GetChunkMeta();
        *chunkSpec.mutable_chunk_meta() = chunkMeta;

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
        chunkSpec.set_erasure_codec(miscExt.erasure_codec());
        chunkSpec.set_striped_erasure(miscExt.striped_erasure());

        return chunkSpec;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateShallowMergeJob(IJobHostPtr host)
{
    return New<TShallowMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
