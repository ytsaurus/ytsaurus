#pragma once

#include "public.h"

#include <yt/yt/library/s3/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TS3MediumStub)

// TODO(achulkov2): This is a temporary stub for an interface which should be
// provided over the actual S3 medium master object.
class TS3MediumStub
    : public TRefCounted
{
public:
    TS3MediumStub(NS3::IClientPtr client, TString bucketName);

    // TODO(achulkov2): Do not forget to ensure that the returned client is started.
    NS3::IClientPtr GetClient() const;

    struct TS3ObjectPlacement
    {
        TString Bucket;
        TString Key;
    };
    // TODO(achulkov2): Allow sharding chunks across multiple S3 buckets.
    // Should we create buckets on the fly somehow? Or ask for a static list
    // of allowed buckets in the config?
    TS3ObjectPlacement GetChunkPlacement(TChunkId sessionId) const;
    static TS3ObjectPlacement GetChunkMetaPlacement(const TS3ObjectPlacement& chunkPlacement);

private:
    const NS3::IClientPtr Client_;
    const TString BucketName_;
};

DEFINE_REFCOUNTED_TYPE(TS3MediumStub)

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient