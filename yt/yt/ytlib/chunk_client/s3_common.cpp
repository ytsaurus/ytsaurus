#include "s3_common.h"
#include "private.h"
#include "session_id.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

TS3MediumStub::TS3MediumStub(
    NS3::IClientPtr client,
    TString bucketName)
    : Client_(std::move(client))
    , BucketName_(std::move(bucketName))
{
    YT_VERIFY(Client_);
}

NS3::IClientPtr TS3MediumStub::GetClient() const
{
    return Client_;
}

TS3MediumStub::TS3ObjectPlacement TS3MediumStub::GetChunkPlacement(TChunkId sessionId) const
{
    return {
        .Bucket = BucketName_,
        .Key = Format("chunk-data/%v", sessionId.ChunkId)
    };
}

TS3MediumStub::TS3ObjectPlacement TS3MediumStub::GetChunkMetaPlacement(const TS3ObjectPlacement& chunkPlacement)
{
    return {
        .Bucket = chunkPlacement.Bucket,
        .Key = chunkPlacement.Key + ChunkMetaSuffix,
    };
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient