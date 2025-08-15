#include "offshore_data_gateway_service.h"

#include "private.h"

#include <yt/yt/server/lib/node_service/helpers.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/s3_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NOffshoreDataGateway {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NDataNode;
using namespace NConcurrency;
using namespace NRpc;

using google::protobuf::RepeatedPtrField;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayService
    : public TServiceBase
{
public:
    TOffshoreDataGatewayService(
        IInvokerPtr invoker,
        IInvokerPtr storageInvoker,
        IAuthenticatorPtr authenticator,
        TMediumDirectoryPtr mediumDirectory)
        : TServiceBase(
            std::move(invoker),
            // TOffshoreDataGatewayService implements a subset of TDataNodeServiceProxy's
            // methods, so we use its descriptor here; read more in the docs of
            // TDataNodeServiceProxy.
            TDataNodeServiceProxy::GetDescriptor(),
            OffshoreDataGatewayLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , StorageInvoker_(std::move(storageInvoker))
        , MediumDirectory_(std::move(mediumDirectory))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSlices));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

private:
    IInvokerPtr StorageInvoker_;
    TMediumDirectoryPtr MediumDirectory_;

    template <typename RequestType>
    IChunkReaderPtr CreateS3ReaderForRequest(
        const RequestType& request,
        TS3ReaderConfigPtr s3ReaderConfig,
        TChunkId chunkId)
    {
        auto replicaWithMedium = FromProto<TChunkReplicaWithMedium>(request.replica_spec());
        auto mediumDescriptor = MediumDirectory_
            ->GetByIndexOrThrow(replicaWithMedium.GetMediumIndex())
            ->template As<TS3MediumDescriptor>();

        THROW_ERROR_EXCEPTION_IF(!mediumDescriptor, "Medium %v is not an S3 medium", replicaWithMedium.GetMediumIndex());

        // Right now meta persistence is not always specified properly for non-external offshore chunks because it would
        // require refactoring existing replica-serialization code (e.g. in chunk merger). To simplify our life, we do not
        // require chunk format to be specified for "native" offshore chunks, i.e. ones with no source URI.
        // TODO(achulkov2): Eliminate this exception once we ensure proper meta persistence specification everywhere.
        bool isChunkFormatRequired = replicaWithMedium.GetMetaPersistence() == EChunkMetaPersistence::None && !replicaWithMedium.GetSourceUri().empty();

        auto chunkFormat = EChunkFormat::Unknown;

        if (request.has_chunk_format()) {
            chunkFormat = FromProto<EChunkFormat>(request.chunk_format());
        }

        if (isChunkFormatRequired && chunkFormat == EChunkFormat::Unknown) {
            THROW_ERROR_EXCEPTION(
                "Cannot read offshore chunk %v with meta persistence %Qlv and source URI %Qv without a well-defined chunk format",
                chunkId,
                replicaWithMedium.GetMetaPersistence(),
                replicaWithMedium.GetSourceUri());
        }

        return CreateS3Reader(
            std::move(mediumDescriptor),
            s3ReaderConfig,
            chunkId,
            chunkFormat,
            replicaWithMedium);
    }

    template <typename RequestsType>
    THashMap<TChunkId, IChunkReaderPtr> CreateS3ReadersForRequests(
        const RequestsType& requests,
        TS3ReaderConfigPtr s3ReaderConfig)
    {
        THashMap<TChunkId, IChunkReaderPtr> readers;
        for (const auto& request: requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            if (readers.contains(chunkId)) {
                continue;
            }

            readers[chunkId] = CreateS3ReaderForRequest(
                request,
                s3ReaderConfig,
                chunkId);
        }

        return readers;
    }

    template <class RequestType>
    TFuture<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> GetChunkMetasForRequests(
        const RepeatedPtrField<RequestType>& requests,
        THashMap<TChunkId, IChunkReaderPtr>& s3Readers)
    {
        std::vector<TFuture<TRefCountedChunkMetaPtr>> chunkMetaFutures;
        for (const auto& request : requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            chunkMetaFutures.push_back(s3Readers[chunkId]->GetMeta({}));
        }
        return AllSet(chunkMetaFutures);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        auto samplingPolicy = FromProto<ESamplingPolicy>(request->sampling_policy());
        auto keyColumns = FromProto<TKeyColumns>(request->key_columns());
        auto requestCount = request->sample_requests_size();
        auto maxSampleSize = request->max_sample_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("SamplingPolicy: %v, KeyColumns: %v, RequestCount: %v, MaxSampleSize: %v, Workload: %v",
            samplingPolicy,
            keyColumns,
            requestCount,
            maxSampleSize,
            workloadDescriptor);

        auto s3Readers = CreateS3ReadersForRequests(
            request->sample_requests(),
            New<TS3ReaderConfig>());

        // TODO(pavel-bash): Now the process of individual samples retrieval happens only when we
        // have read all the chunk metas for all the requests. It would be better if we refactor this
        // part, so that the sample retrieval will be performed right after its chunk meta is read.
        GetChunkMetasForRequests(request->sample_requests(), s3Readers)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }
                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto errors = ProcessGetTableSamplesRequest(
                    *request,
                    *response,
                    requestCount,
                    samplingPolicy,
                    keyColumns,
                    maxSampleSize,
                    results);
                for (const auto& error: errors) {
                    YT_LOG_WARNING(error, "Error building table samples");
                }

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetColumnarStatistics)
    {
        auto requestCount = request->subrequests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);
        auto s3Readers = CreateS3ReadersForRequests(
            request->subrequests(),
            New<TS3ReaderConfig>());
        GetChunkMetasForRequests(request->subrequests(), s3Readers)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto errors = ProcessGetColumnarStatisticsRequest(*request, *response, requestCount, results);
                for (const auto& error: errors) {
                    YT_LOG_WARNING(error, "Error building columnar statistics");
                }

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSlices)
    {
        auto requestCount = request->slice_requests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        auto s3Readers = CreateS3ReadersForRequests(
            request->slice_requests(),
            New<TS3ReaderConfig>());

        GetChunkMetasForRequests(request->slice_requests(), s3Readers)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto errors = ProcessGetChunkSlicesRequest(*request, *response, requestCount, results);
                for (const auto& error: errors) {
                    YT_LOG_WARNING(error, "Error building chunk slices");
                }

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        context->SetRequestInfo("ChunkId: %v, BlockIndexes: %v",
            chunkId,
            blockIndexes);

        auto reader = CreateS3ReaderForRequest(*request, New<TS3ReaderConfig>(), chunkId);
        reader->ReadBlocks({}, blockIndexes)
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& blocks = resultsError.Value();
                YT_VERIFY(blocks.size() == blockIndexes.size());

                response->set_has_complete_chunk(true);
                response->set_net_throttling(false);
                response->set_net_queue_size(0);
                response->set_disk_throttling(false);
                response->set_disk_queue_size(0);

                SetRpcAttachedBlocks(response, blocks);

                context->SetResponseInfo("BlockCount: %v", blocks.size());

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto firstBlockIndex = request->first_block_index();
        auto blockCount = request->block_count();

        context->SetRequestInfo("ChunkId: %v, FirstBlockIndex: %v, BlockCount: %v",
            chunkId,
            firstBlockIndex,
            blockCount);

        auto reader = CreateS3ReaderForRequest(*request, New<TS3ReaderConfig>(), chunkId);
        reader->ReadBlocks({}, firstBlockIndex, blockCount)
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& blocks = resultsError.Value();
                // We are allowed to return less blocks than requested by the GetBlockRange specification.
                YT_VERIFY(std::ssize(blocks) <= blockCount);

                response->set_has_complete_chunk(true);
                response->set_net_throttling(false);
                response->set_net_queue_size(0);
                response->set_disk_throttling(false);
                response->set_disk_queue_size(0);

                SetRpcAttachedBlocks(response, blocks);

                context->SetResponseInfo("BlockCount: %v", blocks.size());

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %v", chunkId);

        auto reader = CreateS3ReaderForRequest(*request, New<TS3ReaderConfig>(), chunkId);
        reader->GetMeta({})
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& meta = resultsError.Value();

                *response->mutable_chunk_meta() = static_cast<NChunkClient::NProto::TChunkMeta>(*meta);

                context->SetResponseInfo("MetaSize: %v", response->chunk_meta().ByteSize());

                context->Reply();
            })
                .Via(StorageInvoker_));
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOffshoreDataGatewayService(
    IInvokerPtr invoker,
    IInvokerPtr storageInvoker,
    IAuthenticatorPtr authenticator,
    TMediumDirectoryPtr mediumDirectory)
{
    return New<TOffshoreDataGatewayService>(std::move(invoker), std::move(storageInvoker), std::move(authenticator), std::move(mediumDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
