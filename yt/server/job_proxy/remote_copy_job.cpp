#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"

#include <server/chunk_server/public.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_helpers.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/erasure_reader.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/erasure_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/meta_state/master_channel.h>
#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <fstream>
#include <iostream>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyJob
    : public TJob
{
public:
    explicit TRemoteCopyJob(IJobHost* host)
        : TJob(host)
        , JobSpec_(host->GetJobSpec())
        , SchedulerJobSpecExt_(JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
        , RemoteNodeDirectory_(New<TNodeDirectory>())
        , CopiedChunks_(0)
    {
        auto config = host->GetConfig();
        ReaderConfig_ = config->JobIO->TableReader;
        WriterConfig_ = config->JobIO->TableWriter;

        YCHECK(SchedulerJobSpecExt_.input_specs_size() == 1);
        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);

        for (const auto& inputChunkSpec : SchedulerJobSpecExt_.input_specs(0).chunks()) {
            YCHECK(!inputChunkSpec.has_start_limit());
            YCHECK(!inputChunkSpec.has_end_limit());
        }

        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
                TYsonString(SchedulerJobSpecExt_.output_specs(0).table_writer_options()));
        OutputChunkListId_ = FromProto<TChunkListId>(
                SchedulerJobSpecExt_.output_specs(0).chunk_list_id());

        {
            auto remoteCopySpec = JobSpec_.GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
            auto remoteMasterConfig = ConvertTo<NMetaState::TMasterDiscoveryConfigPtr>(TYsonString(remoteCopySpec.master_discovery_config()));
            NetworkName_ = Stroka(remoteCopySpec.network_name());
            RemoteMasterChannel_ = CreateLeaderChannel(remoteMasterConfig);
            RemoteNodeDirectory_->MergeFrom(remoteCopySpec.remote_node_directory());
        }
    }

    virtual NJobTrackerClient::NProto::TJobResult Run() override
    {
        PROFILE_TIMING ("/remote_copy_time") {
            for (const auto& inputChunkSpec : SchedulerJobSpecExt_.input_specs(0).chunks()) {
                CopyChunk(inputChunkSpec);
            }
        }

        TJobResult result;
        ToProto(result.mutable_error(), TError());
        return result;
    }

    virtual double GetProgress() const override
    {
        // Caution: progress calculated approximately (assuming all chunks have equal size).
        double chunkProgress = TotalChunkSize_ ? CopiedChunkSize_ / TotalChunkSize_.Get() : 0.0;
        return (CopiedChunks_ + chunkProgress) / SchedulerJobSpecExt_.input_specs(0).chunks_size();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        if (FailedChunkId_) {
            return std::vector<TChunkId>(1, *FailedChunkId_);
        }
        return std::vector<TChunkId>();
    }

    virtual TJobStatistics GetStatistics() const override
    {
        TJobStatistics result;

        result.set_time(GetElapsedTime().MilliSeconds());
        // TODO(ignat): report intermediate statistics for block copying.
        ToProto(result.mutable_input(), DataStatistics_);
        ToProto(result.mutable_output(), DataStatistics_);

        return result;
    }

private:
    const TJobSpec& JobSpec_;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt_;

    IChannelPtr RemoteMasterChannel_;
    Stroka NetworkName_;

    TTableReaderConfigPtr ReaderConfig_;
    TTableWriterConfigPtr WriterConfig_;
    TTableWriterOptionsPtr WriterOptionsTemplate_;

    TChunkListId OutputChunkListId_;

    TNodeDirectoryPtr RemoteNodeDirectory_;

    int CopiedChunks_;
    double CopiedChunkSize_;
    TNullable<double> TotalChunkSize_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;

    TNullable<TChunkId> FailedChunkId_;


    void CopyChunk(const NChunkClient::NProto::TChunkSpec& inputChunkSpec)
    {
        auto host = Host.Lock();
        if (!host) {
            return;
        }

        CopiedChunkSize_ = 0.0;

        auto writerOptions = CloneYsonSerializable(WriterOptionsTemplate_);
        auto inputChunkId = NYT::FromProto<TChunkId>(inputChunkSpec.chunk_id());

        LOG_INFO("Copying chunk %s", ~ToString(inputChunkId));

        auto erasureCodecId = NErasure::ECodec(inputChunkSpec.erasure_codec());
        writerOptions->ErasureCodec = erasureCodecId;

        auto inputReplicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(inputChunkSpec.replicas());

        bool isErasure = IsErasureChunkId(inputChunkId);

        LOG_INFO("Creating output chunk");

        // Create output chunk
        TChunkId outputChunkId;
        std::vector<TChunkReplica> replicas;
        std::vector<TNodeDescriptor> targets;
        {
            auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
            auto writerNodeDirectory = New<TNodeDirectory>();

            auto rsp = CreateChunk(
                host->GetMasterChannel(),
                WriterConfig_,
                writerOptions,
                isErasure ? EObjectType::ErasureChunk : EObjectType::Chunk,
                transactionId).Get();

            OnChunkCreated(
                rsp,
                WriterConfig_,
                writerOptions,
                &outputChunkId,
                &replicas,
                writerNodeDirectory);

            targets = writerNodeDirectory->GetDescriptors(replicas);
        }

        // Copy chunk
        LOG_INFO("Copying blocks");

        NChunkClient::NProto::TChunkInfo chunkInfo;
        NChunkClient::NProto::TChunkMeta chunkMeta;

        if (isErasure) {
            auto erasureCodec = NErasure::GetCodec(erasureCodecId);

            auto readers = CreateErasureAllPartsReaders(
                ReaderConfig_,
                host->GetBlockCache(),
                RemoteMasterChannel_,
                RemoteNodeDirectory_,
                inputChunkId,
                inputReplicas,
                erasureCodec,
                NetworkName_);

            chunkMeta = GetChunkMeta(readers.front());

            auto writers = CreateErasurePartWriters(
                WriterConfig_,
                outputChunkId,
                erasureCodec,
                targets,
                EWriteSessionType::User);

            YCHECK(readers.size() == writers.size());

            auto erasurePlacementExt = GetProtoExtension<TErasurePlacementExt>(chunkMeta.extensions());

            // Thi is upper bound for total size.
            TotalChunkSize_ =
                GetProtoExtension<TMiscExt>(chunkMeta.extensions()).compressed_data_size() +
                erasurePlacementExt.parity_block_count() * erasurePlacementExt.parity_block_size() * erasurePlacementExt.parity_part_count();

            i64 diskSpace = 0;
            for (int i = 0; i < static_cast<int>(readers.size()); ++i) {
                int blockCount = (i < erasureCodec->GetDataPartCount())
                    ? erasurePlacementExt.part_infos(i).block_sizes_size()
                    : erasurePlacementExt.parity_block_count();
                DoCopy(readers[i], writers[i], blockCount, chunkMeta);
                diskSpace += writers[i]->GetChunkInfo().disk_space();
            }
            chunkInfo.set_disk_space(diskSpace);

        } else {
            auto reader = CreateReplicationReader(
                ReaderConfig_,
                host->GetBlockCache(),
                RemoteMasterChannel_,
                RemoteNodeDirectory_,
                Null,
                inputChunkId,
                TChunkReplicaList(),
                NetworkName_);

            chunkMeta = GetChunkMeta(reader);

            auto writer = CreateReplicationWriter(
                WriterConfig_,
                outputChunkId,
                targets,
                EWriteSessionType::User);

            auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta.extensions());
            int blockCount = static_cast<int>(blocksExt.blocks_size());

            TotalChunkSize_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions()).compressed_data_size();

            DoCopy(reader, writer, blockCount, chunkMeta);

            chunkInfo = writer->GetChunkInfo();
        }

        // Prepare data statistics
        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
        
        NChunkClient::NProto::TDataStatistics chunkStatistics;
        chunkStatistics.set_compressed_data_size(miscExt.compressed_data_size());
        chunkStatistics.set_uncompressed_data_size(miscExt.uncompressed_data_size());
        chunkStatistics.set_row_count(miscExt.row_count());
        chunkStatistics.set_chunk_count(1);
        DataStatistics_ += chunkStatistics;

        TObjectServiceProxy objectProxy(host->GetMasterChannel());

        // Confirm
        LOG_INFO("Confirming output chunk");
        {
            static const yhash_set<int> masterMetaTags({
                TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
                TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value });

            auto masterChunkMeta = chunkMeta;
            FilterProtoExtensions(
                masterChunkMeta.mutable_extensions(),
                chunkMeta.extensions(),
                masterMetaTags);

            auto confirmReq = TChunkYPathProxy::Confirm(
                NCypressClient::FromObjectId(outputChunkId));
            NMetaState::GenerateMutationId(confirmReq);
            *confirmReq->mutable_chunk_info() = chunkInfo;
            *confirmReq->mutable_chunk_meta() = masterChunkMeta;
            NYT::ToProto(confirmReq->mutable_replicas(), replicas);

            auto confirmReqRsp = objectProxy.Execute(confirmReq).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(*confirmReqRsp, "Failed to confirm chunk");
        }

        // Attach
        LOG_INFO("Attaching output chunk");
        {
            auto attachReq = TChunkListYPathProxy::Attach(NCypressClient::FromObjectId(OutputChunkListId_));
            ToProto(attachReq->add_children_ids(), outputChunkId);
            NMetaState::GenerateMutationId(attachReq);
            auto attachReqRsp = objectProxy.Execute(attachReq).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(*attachReqRsp, "Failed to attach chunk");
        }
    }

    void DoCopy(
        const NChunkClient::IAsyncReaderPtr& reader,
        const NChunkClient::IAsyncWriterPtr& writer,
        int blockCount,
        const NChunkClient::NProto::TChunkMeta& meta)
    {
        writer->Open();

        for (int i = 0; i < blockCount; ++i) {
            auto blocksRsp = reader->AsyncReadBlocks(std::vector<int>(1, i)).Get();
            if (!blocksRsp.IsOK()) {
                FailedChunkId_ = reader->GetChunkId();
                THROW_ERROR_EXCEPTION_IF_FAILED(blocksRsp, "Failed to read block");
            }
            auto block = blocksRsp.Value().front();
            CopiedChunkSize_ += block.Size();
            if (!writer->WriteBlock(block)) {
                auto rsp = writer->GetReadyEvent().Get();
                THROW_ERROR_EXCEPTION_IF_FAILED(rsp, "Failed to write block");
            }
        }

        auto writerCloseRsp = writer->AsyncClose(meta).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(writerCloseRsp, "Failed to close chunk");
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    NChunkClient::NProto::TChunkMeta GetChunkMeta(const NChunkClient::IAsyncReaderPtr& reader)
    {
        auto inputChunkMetaRsp = reader->AsyncGetChunkMeta().Get();
        if (!inputChunkMetaRsp.IsOK()) {
            FailedChunkId_ = reader->GetChunkId();
            THROW_ERROR_EXCEPTION_IF_FAILED(inputChunkMetaRsp, "Failed to get chunk meta");
        }
        return inputChunkMetaRsp.Value();
    }
};

TJobPtr CreateRemoteCopyJob(IJobHost* host)
{
    return New<TRemoteCopyJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
