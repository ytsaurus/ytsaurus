#include "physical_chunk_reader.h"

#include "replication_reader.h"
#include "s3_reader.h"
#include "chunk_reader_host.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/chunk_client/helpers.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreatePhysicalChunkReader(
    TPhysicalChunkReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    const NProto::TChunkSpec& chunkSpec,
    TChunkReplicaWithMediumList seedReplicas)
{
    if (!chunkReaderHost) {
        THROW_ERROR_EXCEPTION(
            "Cannot create chunk reader for chunk %v without chunk reader host",
            chunkId);
            // TODO(achulkov2): [PForReview] Make replicas serializable.
            // << TErrorAttribute("seed_replicas", seedReplicas);
    }

    auto chunkFormat = EChunkFormat::Unknown;
    if (chunkSpec.has_chunk_meta()) {
        chunkFormat = NYT::FromProto<EChunkFormat>(chunkSpec.chunk_meta().format());
    }

    const auto& nativeConnection = chunkReaderHost->Client->GetNativeConnection();
    auto replicasByType = GetReplicasByType(seedReplicas);

    if (replicasByType.DomesticReplicas.empty() &&
        !replicasByType.OffshoreReplicas.empty() &&
        !nativeConnection->GetConfig()->EnableReplicationReaderForOffshoreData)
    {
        // TODO(pavel-bash): now this code path allows the reads to still be done via
        // the native client. Eventually we should get rid of this functionality, as
        // the replication reader part should be the only one, and this is just a safety
        // mechanism. Then this path along with the configuration option should disappear.
        auto mediumDirectory = nativeConnection->GetMediumDirectory();

        auto offshoreReplica = replicasByType.OffshoreReplicas[0];
        auto mediumDescriptor = mediumDirectory->GetByIndexOrThrow(offshoreReplica.GetMediumIndex());

        if (const auto s3MediumDescriptor = mediumDescriptor->As<TS3MediumDescriptor>()) {
            return CreateS3Reader(
                s3MediumDescriptor, config, chunkId,
                chunkFormat,
                offshoreReplica);
        }

        THROW_ERROR_EXCEPTION("The medium %Qv is not supported for reading data", mediumDescriptor->GetName())
            << TErrorAttribute("medium_index", mediumDescriptor->GetIndex());
    }

    return CreateReplicationReader(
        std::move(config),
        std::move(options),
        std::move(chunkReaderHost),
        chunkId,
        std::move(seedReplicas),
        chunkFormat);
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
