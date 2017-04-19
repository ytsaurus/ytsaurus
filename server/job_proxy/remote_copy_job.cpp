#include "remote_copy_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/ytlib/chunk_client/erasure_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/replication_writer.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NJobProxy {

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
    }

    virtual void Initialize() override
    {
        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
            TYsonString(SchedulerJobSpecExt_.output_table_specs(0).table_writer_options()));
        OutputChunkListId_ = FromProto<TChunkListId>(
            SchedulerJobSpecExt_.output_table_specs(0).chunk_list_id());

        auto remoteConnectionConfig = ConvertTo<TNativeConnectionConfigPtr>(TYsonString(RemoteCopyJobSpecExt_.connection_config()));
        RemoteConnection_ = CreateNativeConnection(remoteConnectionConfig);

        RemoteClient_ = RemoteConnection_->CreateNativeClient(TClientOptions(NSecurityClient::JobUserName));
    }

    virtual TJobResult Run() override
    {
        Host_->OnPrepared();

        PROFILE_TIMING ("/remote_copy_time") {
            for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
                for (const auto& inputChunkSpec : dataSliceDescriptor.ChunkSpecs) {
                    CopyChunk(inputChunkSpec);
                }
            }
        }

        TJobResult result;
        ToProto(result.mutable_error(), TError());
        return result;
    }

    virtual void Abort() override
    { }

    virtual double GetProgress() const override
    {
        // Caution: progress calculated approximately (assuming all chunks have equal size).
        double chunkProgress = TotalChunkSize_ ? static_cast<double>(CopiedChunkSize_) / *TotalChunkSize_ : 0.0;
        return (CopiedChunkCount_ + chunkProgress) / DataSliceDescriptors_.size();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkId_
            ? std::vector<TChunkId>(1, *FailedChunkId_)
            : std::vector<TChunkId>();
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        return std::vector<TDataSliceDescriptor>();
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

    INativeConnectionPtr RemoteConnection_;
    INativeClientPtr RemoteClient_;

    int CopiedChunkCount_ = 0;
    i64 CopiedChunkSize_ = 0;
    TNullable<i64> TotalChunkSize_;

    TDataStatistics DataStatistics_;

    TNullable<TChunkId> FailedChunkId_;


    void CopyChunk(const TChunkSpec& inputChunkSpec)
    {
        CopiedChunkSize_ = 0;

        auto writerOptions = CloneYsonSerializable(WriterOptionsTemplate_);
        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());

        LOG_INFO("Copying input chunk (ChunkId: %v)",
            inputChunkId);

        auto erasureCodecId = NErasure::ECodec(inputChunkSpec.erasure_codec());
        writerOptions->ErasureCodec = erasureCodecId;

        auto inputReplicas = NYT::FromProto<TChunkReplicaList>(inputChunkSpec.replicas());
        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        LOG_INFO("Creating output chunk");

        // Create output chunk.
        auto outputChunkId = CreateChunk(
            Host_->GetClient(),
            CellTagFromId(OutputChunkListId_),
            writerOptions,
            transactionId,
            OutputChunkListId_,
            Logger);

        LOG_INFO("Output chunk created (ChunkId: %v)",
            outputChunkId);

        // Copy chunk.
        LOG_INFO("Copying chunk data");

        TChunkInfo chunkInfo;
        TChunkMeta chunkMeta;
        TChunkReplicaList writtenReplicas;

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
                Host_->GetBlockCache());

            chunkMeta = GetChunkMeta(readers.front());

            auto writers = CreateErasurePartWriters(
                WriterConfig_,
                New<TRemoteWriterOptions>(),
                outputChunkId,
                erasureCodec,
                New<TNodeDirectory>(),
                Host_->GetClient());

            YCHECK(readers.size() == writers.size());

            auto erasurePlacementExt = GetProtoExtension<TErasurePlacementExt>(chunkMeta.extensions());

            // Compute an upper bound for total size.
            TotalChunkSize_ =
                GetProtoExtension<TMiscExt>(chunkMeta.extensions()).compressed_data_size() +
                erasurePlacementExt.parity_block_count() * erasurePlacementExt.parity_block_size() * erasurePlacementExt.parity_part_count();

            i64 diskSpace = 0;
            for (int i = 0; i < static_cast<int>(readers.size()); ++i) {
                int blockCount = (i < erasureCodec->GetDataPartCount())
                    ? erasurePlacementExt.part_infos(i).block_sizes_size()
                    : erasurePlacementExt.parity_block_count();

                // ToDo(psushin): copy chunk parts is parallel.
                DoCopy(readers[i], writers[i], blockCount, chunkMeta);
                diskSpace += writers[i]->GetChunkInfo().disk_space();

                auto replicas = writers[i]->GetWrittenChunkReplicas();
                YCHECK(replicas.size() == 1);
                auto replica = TChunkReplica(
                    replicas.front().GetNodeId(),
                    i,
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
                TChunkReplicaList(),
                Host_->GetBlockCache());

            chunkMeta = GetChunkMeta(reader);

            auto writer = CreateReplicationWriter(
                WriterConfig_,
                New<TRemoteWriterOptions>(),
                outputChunkId,
                TChunkReplicaList(),
                New<TNodeDirectory>(),
                Host_->GetClient());

            auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta.extensions());
            int blockCount = static_cast<int>(blocksExt.blocks_size());

            TotalChunkSize_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions()).compressed_data_size();

            DoCopy(reader, writer, blockCount, chunkMeta);

            chunkInfo = writer->GetChunkInfo();
            writtenReplicas = writer->GetWrittenChunkReplicas();
        }

        // Update data statistics.
        DataStatistics_.set_chunk_count(DataStatistics_.chunk_count() + 1);

        // Confirm chunk.
        LOG_INFO("Confirming output chunk");
        YCHECK(!writtenReplicas.empty());
        {
            static const yhash_set<int> masterMetaTags{
                TProtoExtensionTag<TMiscExt>::Value,
                TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value
            };

            auto masterChunkMeta = chunkMeta;
            FilterProtoExtensions(
                masterChunkMeta.mutable_extensions(),
                chunkMeta.extensions(),
                masterMetaTags);

            auto outputCellTag = CellTagFromId(OutputChunkListId_);
            auto outputMasterChannel = Host_->GetClient()->GetMasterChannelOrThrow(EMasterChannelKind::Leader, outputCellTag);
            TChunkServiceProxy proxy(outputMasterChannel);

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            auto* req = batchReq->add_confirm_chunk_subrequests();
            ToProto(req->mutable_chunk_id(), outputChunkId);
            *req->mutable_chunk_info() = chunkInfo;
            *req->mutable_chunk_meta() = masterChunkMeta;
            NYT::ToProto(req->mutable_replicas(), writtenReplicas);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Failed to confirm chunk %v",
                outputChunkId);
        }
    }

    void DoCopy(
        IChunkReaderPtr reader,
        IChunkWriterPtr writer,
        int blockCount,
        const TChunkMeta& meta)
    {
        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error opening writer");

        for (int i = 0; i < blockCount; ++i) {
            auto asyncResult = reader->ReadBlocks(ReaderConfig_->WorkloadDescriptor, i, 1);
            auto result = WaitFor(asyncResult);
            if (!result.IsOK()) {
                FailedChunkId_ = reader->GetChunkId();
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reading block");
            }

            auto block = result.Value().front();
            CopiedChunkSize_ += block.Size();

            if (!writer->WriteBlock(block)) {
                auto result = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error writing block");
            }

            DataStatistics_.set_compressed_data_size(DataStatistics_.compressed_data_size() + block.Size());
        }

        {
            auto result = WaitFor(writer->Close(meta));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error closing chunk");
        }
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    TChunkMeta GetChunkMeta(IChunkReaderPtr reader)
    {
        auto asyncResult = reader->GetMeta(ReaderConfig_->WorkloadDescriptor);
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

} // namespace NJobProxy
} // namespace NYT
