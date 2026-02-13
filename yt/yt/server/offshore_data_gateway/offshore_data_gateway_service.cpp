#include "offshore_data_gateway_service.h"

#include "private.h"

#include <yt/yt/server/lib/s3/config.h>
#include <yt/yt/server/lib/s3/chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/library/s3/public.h>
#include <yt/yt/library/s3/client.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NOffshoreDataGateway {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NS3;

using google::protobuf::RepeatedPtrField;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayService
    : public TServiceBase
{
public:
    TOffshoreDataGatewayService(
        IInvokerPtr invoker,
        IInvokerPtr storageInvoker,
        IPollerPtr s3Poller,
        IAuthenticatorPtr authenticator,
        TMediumDirectoryPtr mediumDirectory,
        TMediumDirectorySynchronizerPtr mediumDirectorySynchronizer)
        : TServiceBase(
            std::move(invoker),
            // TOffshoreDataGatewayService implements a subset of TDataNodeServiceProxy's
            // methods, so we use its descriptor here.
            TDataNodeServiceProxy::GetDescriptor(),
            OffshoreDataGatewayLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , StorageInvoker_(std::move(storageInvoker))
        , S3Poller_(std::move(s3Poller))
        , MediumDirectory_(std::move(mediumDirectory))
        , MediumDirectorySynchronizer_(std::move(mediumDirectorySynchronizer))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

private:
    const IInvokerPtr StorageInvoker_;
    const IPollerPtr S3Poller_;
    const TMediumDirectoryPtr MediumDirectory_;
    const TMediumDirectorySynchronizerPtr MediumDirectorySynchronizer_;

    template <typename TRequestType>
    IChunkReaderPtr CreateS3Reader(
        const TRequestType& request,
        TS3ReaderConfigPtr s3ReaderConfig,
        TChunkId chunkId)
    {
        auto replicaWithMedium = FromProto<TChunkReplicaWithMedium>(request.replica_spec());

        auto genericMediumDescriptor = MediumDirectory_->FindByIndex(replicaWithMedium.GetMediumIndex());
        if (!genericMediumDescriptor) {
            // If this code is executed a short time after medium creation, this new medium will
            // not appear in the directory until it's synced. That's why we issue another sync
            // if failed to find a medium - maybe it will appear.
            WaitFor(MediumDirectorySynchronizer_->NextSync(true))
                .ThrowOnError();

            genericMediumDescriptor = MediumDirectory_->GetByIndexOrThrow(replicaWithMedium.GetMediumIndex());
        }

        auto mediumDescriptor = genericMediumDescriptor->template As<TS3MediumDescriptor>();
        THROW_ERROR_EXCEPTION_IF(!mediumDescriptor, "Medium %v is not an S3 medium", replicaWithMedium.GetMediumIndex());

        // TODO(discuss in PR): We create the S3 credential provider from the medium as we store the access key ID
        // and the secret access key in its configuration. Here only placement and proxy are stored. How can we get
        // the credentials to access S3 then?
        auto s3CredentialProvider = NS3::CreateAnonymousCredentialProvider();

        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = mediumDescriptor->GetConfig()->Url;
        clientConfig->Region = mediumDescriptor->GetConfig()->Region;

        auto s3Client = CreateClient(
            std::move(clientConfig),
            std::move(s3CredentialProvider),
            /*sslContextConfig*/ nullptr,
            S3Poller_,
            S3Poller_->GetInvoker());

        return CreateS3RegularChunkReader(
            std::move(s3Client),
            mediumDescriptor,
            std::move(s3ReaderConfig),
            chunkId);
    }

    template <typename TRequestsType>
    THashMap<TChunkId, IChunkReaderPtr> CreateS3Readers(
        const TRequestsType& requests,
        TS3ReaderConfigPtr s3ReaderConfig)
    {
        THashMap<TChunkId, IChunkReaderPtr> readers;
        for (const auto& request: requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            if (readers.contains(chunkId)) {
                continue;
            }

            InsertOrCrash(readers, std::pair(
                chunkId,
                CreateS3Reader(
                    request,
                    std::move(s3ReaderConfig),
                    chunkId
                ))
            );
        }

        return readers;
    }

    template <class TRequestType>
    TFuture<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> GetChunkMetasForRequests(
        const RepeatedPtrField<TRequestType>& requests,
        THashMap<TChunkId, IChunkReaderPtr>& s3Readers)
    {
        std::vector<TFuture<TRefCountedChunkMetaPtr>> chunkMetaFutures;
        chunkMetaFutures.reserve(requests.size());

        for (const auto& request : requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            chunkMetaFutures.push_back(s3Readers[chunkId]->GetMeta({}));
        }
        return AllSet(std::move(chunkMetaFutures));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        context->SetRequestInfo("ChunkId: %v, BlockIndexes: %v",
            chunkId,
            blockIndexes);

        auto reader = CreateS3Reader(*request, New<TS3ReaderConfig>(), chunkId);
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

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %v", chunkId);

        auto reader = CreateS3Reader(*request, New<TS3ReaderConfig>(), chunkId);
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
    IPollerPtr s3Poller,
    IAuthenticatorPtr authenticator,
    TMediumDirectoryPtr mediumDirectory,
    TMediumDirectorySynchronizerPtr mediumDirectorySynchronizer)
{
    return New<TOffshoreDataGatewayService>(
        std::move(invoker),
        std::move(storageInvoker),
        std::move(s3Poller),
        std::move(authenticator),
        std::move(mediumDirectory),
        std::move(mediumDirectorySynchronizer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
