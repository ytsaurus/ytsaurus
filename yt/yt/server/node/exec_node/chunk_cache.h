#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactDownloadOptions
{
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NChunkClient::TTrafficMeterPtr TrafficMeter;

    std::vector<TString> WorkloadDescriptorAnnotations;
};

//! Manages chunks cached at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
class TChunkCache
    : public TRefCounted
{
public:
    TChunkCache(
        NDataNode::TDataNodeConfigPtr config,
        IBootstrap* bootstrap);
    ~TChunkCache();

    void Initialize();

    //! Returns |true| if at least one chunk cache location is enabled.
    bool IsEnabled() const;

    //! Finds chunk by id. Returns |nullptr| if no chunk exists.
    NDataNode::IChunkPtr FindChunk(NChunkClient::TChunkId chunkId);

    //! Returns the list of all registered chunks.
    std::vector<NDataNode::IChunkPtr> GetChunks();

    //! Returns the number of registered chunks.
    int GetChunkCount();

    //! Downloads a single- or multi-chunk artifact into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a pre-set result.
     */
    TFuture<NDataNode::IChunkPtr> DownloadArtifact(
        const NDataNode::TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        bool* fetchedFromCache = nullptr);

    //! Constructs a producer that will download the artifact and feed its content to a stream.
    std::function<void(IOutputStream*)> MakeArtifactDownloadProducer(
        const NDataNode::TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions);

    //! Cache locations.
    DECLARE_BYREF_RO_PROPERTY(std::vector<NDataNode::TCacheLocationPtr>, Locations);

    //! Raised when a chunk is added to the cache.
    DECLARE_SIGNAL(void(NDataNode::IChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the cache.
    DECLARE_SIGNAL(void(NDataNode::IChunkPtr), ChunkRemoved);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TChunkCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
