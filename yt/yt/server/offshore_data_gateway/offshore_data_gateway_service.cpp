#include "offshore_data_gateway_service.h"

#include "private.h"

#include <yt/yt/server/lib/s3/config.h>
#include <yt/yt/server/lib/s3/chunk_reader.h>
#include <yt/yt/server/lib/s3/chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
    }

private:
    const IInvokerPtr StorageInvoker_;
    const IPollerPtr S3Poller_;
    const TMediumDirectoryPtr MediumDirectory_;
    const TMediumDirectorySynchronizerPtr MediumDirectorySynchronizer_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SessionsLock_);
    THashMap<TSessionId, IChunkWriterPtr> Sessions_;

    TS3MediumDescriptorPtr GetS3MediumDescriptor(int mediumIndex)
    {
        auto genericMediumDescriptor = MediumDirectory_->FindByIndex(mediumIndex);
        if (!genericMediumDescriptor) {
            // If this code is executed a short time after medium creation, this new medium will
            // not appear in the directory until it's synced. That's why we issue another sync
            // if failed to find a medium - maybe it will appear.
            WaitFor(MediumDirectorySynchronizer_->NextSync(true))
                .ThrowOnError();

            genericMediumDescriptor = MediumDirectory_->GetByIndexOrThrow(mediumIndex);
        }

        auto mediumDescriptor = genericMediumDescriptor->template As<TS3MediumDescriptor>();
        THROW_ERROR_EXCEPTION_IF(!mediumDescriptor, "Medium %v is not an S3 medium", mediumIndex);
        return mediumDescriptor;
    }

    NS3::IClientPtr CreateS3ClientForMedium(const TS3MediumDescriptorPtr& mediumDescriptor)
    {
        const auto& mediumConfig = mediumDescriptor->GetConfig();
        auto s3CredentialProvider = NS3::CreateStaticCredentialProvider(
            mediumConfig->AccessKeyId,
            mediumConfig->SecretAccessKey);

        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = mediumConfig->Url;
        clientConfig->Region = mediumConfig->Region;

        auto s3Client = CreateClient(
            std::move(clientConfig),
            std::move(s3CredentialProvider),
            /*sslContextConfig*/ nullptr,
            S3Poller_,
            S3Poller_->GetInvoker());
        WaitFor(s3Client->Start())
            .ThrowOnError();
        return s3Client;
    }

    template <typename TRequestType>
    IChunkReaderPtr CreateS3Reader(
        const TRequestType& request,
        TS3ReaderConfigPtr s3ReaderConfig,
        TChunkId chunkId)
    {
        auto replicaWithMedium = FromProto<TChunkReplicaWithMedium>(request.replica_spec());
        auto mediumDescriptor = GetS3MediumDescriptor(replicaWithMedium.GetMediumIndex());
        auto s3Client = CreateS3ClientForMedium(mediumDescriptor);
        return CreateS3RegularChunkReader(
            std::move(s3Client),
            mediumDescriptor,
            std::move(s3ReaderConfig),
            chunkId);
    }

    IChunkWriterPtr FindSession(TSessionId sessionId)
    {
        auto guard = ReaderGuard(SessionsLock_);
        auto it = Sessions_.find(sessionId);
        if (it == Sessions_.end()) {
            return nullptr;
        }
        return it->second;
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
                if (context->IsCanceled()) {
                    return;
                }

                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
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
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();

        context->SetRequestInfo("ChunkId: %v, FirstBlockIndex: %v, BlockCount: %v",
            chunkId,
            firstBlockIndex,
            blockCount);

        auto reader = CreateS3Reader(*request, New<TS3ReaderConfig>(), chunkId);
        reader->ReadBlocks({}, firstBlockIndex, blockCount)
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& resultsError) {
                if (context->IsCanceled()) {
                    return;
                }

                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                const auto& blocks = resultsError.Value();

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
                if (context->IsCanceled()) {
                    return;
                }

                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                const auto& meta = resultsError.Value();

                *response->mutable_chunk_meta() = static_cast<NChunkClient::NProto::TChunkMeta>(*meta);

                context->SetResponseInfo("MetaSize: %v", response->chunk_meta().ByteSize());

                context->Reply();
            })
                .Via(StorageInvoker_));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v", sessionId);

        auto mediumDescriptor = GetS3MediumDescriptor(sessionId.MediumIndex);
        auto s3Client = CreateS3ClientForMedium(mediumDescriptor);

        auto writer = NS3::CreateS3RegularChunkWriter(
            std::move(s3Client),
            mediumDescriptor,
            New<TS3WriterConfig>(),
            sessionId);

        WaitFor(writer->Open())
            .ThrowOnError();

        {
            auto guard = WriterGuard(SessionsLock_);
            Sessions_[sessionId] = writer;
        }

        // No memory probing needed for S3 — the upload window is managed internally.
        response->set_use_probe_put_blocks(false);

        context->SetResponseInfo("SessionId: %v", sessionId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();

        context->SetRequestInfo("SessionId: %v, FirstBlockIndex: %v, BlockCount: %v",
            sessionId,
            firstBlockIndex,
            request->Attachments().size());

        auto writer = FindSession(sessionId);
        THROW_ERROR_EXCEPTION_IF(!writer, "No such write session %v", sessionId);

        auto blocks = GetRpcAttachedBlocks(request, /*validateChecksums*/ true);

        // WriteBlocks returns false when the upload window is full.
        // We must wait for the ready event before responding so the client
        // knows it can send more data.
        if (!writer->WriteBlocks({}, {}, blocks)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }

        response->set_close_demanded(false);

        context->SetResponseInfo("SessionId: %v, FirstBlockIndex: %v", sessionId, firstBlockIndex);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v, BlockIndex: %v",
            sessionId,
            request->block_index());

        // S3 uploads are asynchronous — no explicit flush step is needed.
        response->set_close_demanded(false);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v, BlockCount: %v",
            sessionId,
            request->block_count());

        IChunkWriterPtr writer;
        {
            auto guard = WriterGuard(SessionsLock_);
            auto it = Sessions_.find(sessionId);
            if (it == Sessions_.end()) {
                if (request->ignore_missing_session()) {
                    context->Reply();
                    return;
                }
                THROW_ERROR_EXCEPTION("No such write session %v", sessionId);
            }
            writer = it->second;
            Sessions_.erase(it);
        }

        // The meta arriving here has already been finalized by TReplicationWriter.
        auto deferredMeta = New<TDeferredChunkMeta>();
        deferredMeta->CopyFrom(request->chunk_meta());
        deferredMeta->Finalize();

        WaitFor(writer->Close({}, {}, deferredMeta))
            .ThrowOnError();

        *response->mutable_chunk_info() = writer->GetChunkInfo();

        context->SetResponseInfo("SessionId: %v, DiskSpace: %v",
            sessionId,
            response->chunk_info().disk_space());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CancelChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v", sessionId);

        IChunkWriterPtr writer;
        {
            auto guard = WriterGuard(SessionsLock_);
            auto it = Sessions_.find(sessionId);
            if (it != Sessions_.end()) {
                writer = it->second;
                Sessions_.erase(it);
            }
        }

        if (writer) {
            YT_UNUSED_FUTURE(writer->Cancel());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        // Offshore sessions do not have disk or memory pressure.
        response->set_close_demanded(false);
        response->set_net_throttling(false);
        response->set_net_queue_size(0);
        context->Reply();
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
