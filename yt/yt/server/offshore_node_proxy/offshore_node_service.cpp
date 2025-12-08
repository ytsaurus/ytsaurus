#include "offshore_node_service.h"

#include "private.h"

#include <yt/yt/server/lib/node_service/helpers.h>

#include <yt/yt/ytlib/chunk_client/offshore_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/s3_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NOffshoreNodeProxy {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NDataNode;
using namespace NConcurrency;
using namespace NRpc;

using google::protobuf::RepeatedPtrField;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeService
    : public TServiceBase
{
public:
    TOffshoreNodeService(
        IInvokerPtr invoker,
        IInvokerPtr storageInvoker,
        IAuthenticatorPtr authenticator,
        TMediumDirectoryPtr mediumDirectory)
        : TServiceBase(
            std::move(invoker),
            TOffshoreNodeServiceProxy::GetDescriptor(),
            OffshoreNodeProxyLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , StorageInvoker_(std::move(storageInvoker))
        , MediumDirectory_(std::move(mediumDirectory))
    {
        // TODO(pavel-bash): When we need more methods (like GetBlockSet or GetChunkMeta), we can
        // retrieve their implementation from the commit 506d15f.
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSlices));
    }

private:
    IInvokerPtr StorageInvoker_;
    TMediumDirectoryPtr MediumDirectory_;

    template <typename RequestType>
    THashMap<TChunkId, IChunkReaderPtr> CreateS3ReadersForRequests(
        const RepeatedPtrField<RequestType>& requests,
        TS3ReaderConfigPtr s3ReaderConfig)
    {
        THashMap<TChunkId, IChunkReaderPtr> readers;
        for (const auto& request: requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            if (readers.contains(chunkId)) {
                continue;
            }

            auto replicaWithMedium = FromProto<TChunkReplicaWithMedium>(request.replica_spec());
            auto mediumDescriptor = MediumDirectory_
                ->GetByIndexOrThrow(replicaWithMedium.GetMediumIndex())
                ->template As<TS3MediumDescriptor>();

            auto sourceUri = std::string(replicaWithMedium.GetSourceUri());
            auto chunkFormat = EChunkFormat::Unknown;
            if (!sourceUri.empty()) {
                chunkFormat = GetChunkFormatFromExternalSourceFormat(
                    DeduceExternalSourceFormatOrThrow(sourceUri));
            }

            readers[chunkId] = CreateS3Reader(
                std::move(mediumDescriptor),
                s3ReaderConfig,
                chunkId,
                chunkFormat,
                std::move(sourceUri));
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
            }).Via(StorageInvoker_));
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
            }).Via(StorageInvoker_));
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
            }).Via(StorageInvoker_));
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOffshoreNodeService(
    IInvokerPtr invoker,
    IInvokerPtr storageInvoker,
    IAuthenticatorPtr authenticator,
    TMediumDirectoryPtr mediumDirectory)
{
    return New<TOffshoreNodeService>(std::move(invoker), std::move(storageInvoker), std::move(authenticator), std::move(mediumDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
