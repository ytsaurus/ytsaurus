#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"

#include <server/chunk_server/public.h>

#include <ytlib/api/connection.h>

#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/chunk_helpers.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/erasure_reader.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/erasure_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>

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
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
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
            YCHECK(!inputChunkSpec.has_lower_limit());
            YCHECK(!inputChunkSpec.has_upper_limit());
        }

        WriterOptionsTemplate_ = ConvertTo<TTableWriterOptionsPtr>(
                TYsonString(SchedulerJobSpecExt_.output_specs(0).table_writer_options()));
        OutputChunkListId_ = FromProto<TChunkListId>(
                SchedulerJobSpecExt_.output_specs(0).chunk_list_id());

        {
            auto remoteCopySpec = JobSpec_.GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
            auto remoteConnectionConfig = ConvertTo<NApi::TConnectionConfigPtr>(TYsonString(remoteCopySpec.connection_config()));
            NetworkName_ = Stroka(remoteCopySpec.network_name());
            RemoteMasterChannel_ = NApi::CreateConnection(remoteConnectionConfig)->GetMasterChannel();
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
        double chunkProgress = TotalChunkSize_ ? CopiedChunkSize_ / *TotalChunkSize_ : 0.0;
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

    TDataStatistics DataStatistics_;

    TNullable<TChunkId> FailedChunkId_;


    void CopyChunk(const TChunkSpec& inputChunkSpec)
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

            auto rsp = WaitFor(CreateChunk(
                host->GetMasterChannel(),
                WriterConfig_,
                writerOptions,
                isErasure ? EObjectType::ErasureChunk : EObjectType::Chunk,
                transactionId));

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

        TChunkInfo chunkInfo;
        TChunkMeta chunkMeta;

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

            // Compute an upper bound for total size.
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
        
        TDataStatistics chunkStatistics;
        chunkStatistics.set_compressed_data_size(miscExt.compressed_data_size());
        chunkStatistics.set_uncompressed_data_size(miscExt.uncompressed_data_size());
        chunkStatistics.set_chunk_count(1);
        DataStatistics_ += chunkStatistics;

        TObjectServiceProxy objectProxy(host->GetMasterChannel());

        // Confirm chunk.
        LOG_INFO("Confirming output chunk");
        {
            static const yhash_set<int> masterMetaTags({
                TProtoExtensionTag<TMiscExt>::Value,
                TProtoExtensionTag<NVersionedTableClient::NProto::TBoundaryKeysExt>::Value });

            auto masterChunkMeta = chunkMeta;
            FilterProtoExtensions(
                masterChunkMeta.mutable_extensions(),
                chunkMeta.extensions(),
                masterMetaTags);

            auto req = TChunkYPathProxy::Confirm(
                NObjectClient::FromObjectId(outputChunkId));
            NHydra::GenerateMutationId(req);
            *req->mutable_chunk_info() = chunkInfo;
            *req->mutable_chunk_meta() = masterChunkMeta;
            NYT::ToProto(req->mutable_replicas(), replicas);

            auto rsp = WaitFor(objectProxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Failed to confirm chunk");
        }

        // Attach chunk.
        LOG_INFO("Attaching output chunk");
        {
            auto req = TChunkListYPathProxy::Attach(NObjectClient::FromObjectId(OutputChunkListId_));
            ToProto(req->add_children_ids(), outputChunkId);
            NHydra::GenerateMutationId(req);

            auto rsp = WaitFor(objectProxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error attaching chunk");
        }
    }

    void DoCopy(
        NChunkClient::IReaderPtr reader,
        NChunkClient::IWriterPtr writer,
        int blockCount,
        const TChunkMeta& meta)
    {
        writer->Open();

        for (int i = 0; i < blockCount; ++i) {
            auto result = WaitFor(reader->ReadBlocks(i, 1));
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
        }

        {
            auto result = WaitFor(writer->Close(meta));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error closing chunk");
        }
    }

    // Request input chunk meta. Input and output chunk metas are the same.
    TChunkMeta GetChunkMeta(NChunkClient::IReaderPtr reader)
    {
        auto result = WaitFor(reader->GetMeta());
        if (!result.IsOK()) {
            FailedChunkId_ = reader->GetChunkId();
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to get chunk meta");
        }
        return result.Value();
    }
};

TJobPtr CreateRemoteCopyJob(IJobHost* host)
{
    return New<TRemoteCopyJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
