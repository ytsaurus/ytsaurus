#include "chunk_replica_destroyer.h"
#include "medium_directory.h"

#include <yt/yt/library/s3/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TS3ChunkReplicaDestroyer
    : public IChunkReplicaDestroyer
{
public:
    TS3ChunkReplicaDestroyer(
        const TChunkId& chunkId,
        const TChunkReplicaWithMedium& chunkReplica,
        const TS3MediumDescriptorPtr& mediumDescriptor)
        : ChunkId_(chunkId)
        , ChunkReplica_(chunkReplica)
        , MediumDescriptor_(mediumDescriptor)
        , Client_(MediumDescriptor_->GetClient())
        // TODO(achulkov2): This will change after we start uploading meta to S3/passing source URI here.
        , ChunkPlacement_(MediumDescriptor_->GetChunkPlacement(
            ChunkId_,
            std::string{chunkReplica.GetSourceUri()}))
        , ChunkMetaPlacement_(MediumDescriptor_->GetChunkMetaPlacement(
            ChunkId_,
            /*externallyAttached*/ !chunkReplica.GetSourceUri().empty()))
        , Logger(ChunkClientLogger().WithTag(
            "ChunkId: %v, ChunkReplica: %v, MediumIndex: %v, MediumName: %v, MediumId: %v",
            chunkId,
            chunkReplica,
            chunkReplica.GetMediumIndex(),
            mediumDescriptor->GetName(),
            mediumDescriptor->GetId()))
    {
        YT_VERIFY(ChunkReplica_.GetNodeId() == NNodeTrackerClient::OffshoreNodeId);
    }

    TFuture<void> Destroy() override
    {
        YT_LOG_DEBUG("Destroying chunk replica in S3");

        return DeleteObjects({
            ChunkPlacement_,
            ChunkMetaPlacement_,
        })
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                YT_LOG_DEBUG("Chunk replica destroyed in S3");
            }));
    }

private:
    const TChunkId ChunkId_;
    const TChunkReplicaWithMedium ChunkReplica_;
    const TS3MediumDescriptorPtr MediumDescriptor_;
    const NS3::IClientPtr Client_;
    const NS3::TObjectDescriptor ChunkPlacement_;
    const NS3::TObjectDescriptor ChunkMetaPlacement_;

    const NLogging::TLogger Logger;

    TFuture<void> DeleteObjects(const std::vector<NS3::TObjectDescriptor>& objects)
    {
        THashMap<TString, NS3::TDeleteObjectsRequest> requestsByBucket;
        for (const auto& object : objects) {
            auto& request = requestsByBucket[object.Bucket()];
            request.Bucket = object.Bucket();
            request.Objects.push_back(object.Key());
        }

        std::vector<TFuture<void>> deleteFutures;
        for (const auto& [bucket, request] : requestsByBucket) {
            YT_LOG_DEBUG("Deleting S3 objects (Bucket: %v, Keys: %v)", bucket, request.Objects);
            deleteFutures.push_back(Client_->DeleteObjects(request)
                .Apply(BIND([bucket, request, Logger = Logger] (const NS3::TDeleteObjectsResponse& response) {
                    for (const auto& error: response.Errors) {
                        if (error.Code != "NoSuchKey") {
                            THROW_ERROR_EXCEPTION("Failed to delete S3 object %Qv with code %Qv: %v",
                                bucket + "/" + error.Key,
                                error.Code,
                                error.Message);
                        } else {
                            YT_LOG_DEBUG("S3 object not found during deletion (Bucket: %v, Key: %v)", bucket, error.Key);
                        }
                    }

                    // TODO(achulkov2): Add deleted objects parsing to response and only actually deleted objects.
                    YT_LOG_DEBUG("S3 objects deleted (Bucket: %v, Keys: %v)", bucket, request.Objects);
                })));
        }

        return AllSucceeded(std::move(deleteFutures));
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaDestroyerPtr CreateChunkReplicaDestroyer(
    const TChunkId& chunkId,
    const TChunkReplicaWithMedium& chunkReplica,
    const TMediumDirectoryPtr& mediumDirectory)
{
    auto mediumDescriptor = mediumDirectory->FindByIndex(chunkReplica.GetMediumIndex());

    if (!mediumDescriptor) {
        THROW_ERROR_EXCEPTION(
            "Cannot destroy chunk replica %v of chunk %v: medium %v is missing in medium directory",
            chunkReplica,
            chunkId,
            chunkReplica.GetMediumIndex());
    }

    if (auto s3MediumDescriptor = mediumDescriptor->As<TS3MediumDescriptor>()) {
        return New<TS3ChunkReplicaDestroyer>(chunkId, chunkReplica, s3MediumDescriptor);
    }

    THROW_ERROR_EXCEPTION(
        "Cannot destroy chunk replica %v of chunk %v: medium %v(%v) is not supported",
        chunkReplica,
        chunkId,
        chunkReplica.GetMediumIndex(),
        mediumDescriptor->GetName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
