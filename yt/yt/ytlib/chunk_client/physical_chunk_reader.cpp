#include "physical_chunk_reader.h"
#include "replication_reader.h"
#include "s3_reader.h"
#include "chunk_reader_host.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt/yt/client/chunk_client/helpers.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreatePhysicalChunkReader(
    TPhysicalChunkReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaWithMediumList seedReplicas)
{
    auto replicasByType = GetReplicasByType(seedReplicas);

    if (replicasByType.DomesticReplicas.empty() && !replicasByType.OffshoreReplicas.empty()) {
        if (!chunkReaderHost) {
            THROW_ERROR_EXCEPTION(
                "Cannot create offshore chunk reader for chunk %v without chunk reader host",
                chunkId);
                // TODO(achulkov2): [PForReview] Make replicas serializable.
                // << TErrorAttribute("seed_replicas", seedReplicas);
        }

        auto mediumDirectory = chunkReaderHost->Client->GetNativeConnection()->GetMediumDirectory();

        auto mediumDescriptor = mediumDirectory->GetByIndexOrThrow(replicasByType.OffshoreReplicas[0].GetMediumIndex());

        if (const auto s3MediumDescriptor = mediumDescriptor->As<TS3MediumDescriptor>()) {
            return CreateS3Reader(s3MediumDescriptor, config, chunkId);
        }

        THROW_ERROR_EXCEPTION("The medium %Qv is not supported for reading data", mediumDescriptor->GetName())
            << TErrorAttribute("medium_index", mediumDescriptor->GetIndex());
    }

    return CreateReplicationReader(
        std::move(config),
        std::move(options),
        std::move(chunkReaderHost),
        chunkId,
        std::move(replicasByType.DomesticReplicas));
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreatePhysicalChunkReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IThroughputThrottlerPtr mediumThrottler)
{
    if (auto replicationReader = TryCreateReplicationReaderThrottlingAdapter(underlyingReader, bandwidthThrottler, rpsThrottler, mediumThrottler)) {
        return replicationReader;
    }
    
    if (auto s3Reader = TryCreateS3ReaderThrottlingAdapter(underlyingReader, bandwidthThrottler, rpsThrottler, mediumThrottler)) {
        return s3Reader;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
