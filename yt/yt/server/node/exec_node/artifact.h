#pragma once

#include <yt/yt/server/node/exec_node/artifact.pb.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/cpu_clock/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECacheEventType,
    (Miss)
    (Hit)
    (AsyncHit)
);

//! Mutable outcome of an artifact download; finalized by the artifact cache
//! before the returned future is set.
struct TCacheEventState final
{
    ECacheEventType CacheEventType = ECacheEventType::Miss;
};

////////////////////////////////////////////////////////////////////////////////

struct TArtifactDownloadContext
{
    NChunkClient::TTrafficMeterPtr TrafficMeter;
    std::vector<std::string> WorkloadDescriptorAnnotations;

    //! Called when a layer is served from the layer volume cache (tmpfs, SLRU or SquashFS volume cache)
    //! instead of being fetched over the network.
    TCallback<void(i64 compressedDataSize)> OnLayerCacheHit;

    //! Called when a layer is served from the local chunk cache instead of being fetched over the network.
    TCallback<void(i64 compressedDataSize)> OnChunkCacheHit;

    //! Called when a layer was awaited while another job was inserting it into the layer volume cache;
    //! asyncHitCpuDuration is the time the concurrent insertion was awaited.
    TCallback<void(TCpuDuration asyncHitCpuDuration, i64 compressedDataSize)> OnLayerCacheAsyncHit;

    //! Called when a layer was awaited while another job was fetching it into the chunk cache;
    //! asyncHitCpuDuration is the time the concurrent fetch was awaited.
    TCallback<void(TCpuDuration asyncHitCpuDuration, i64 compressedDataSize)> OnChunkCacheAsyncHit;

    //! Called after a layer artifact is fetched over the network; skipped when it is served locally.
    //! downloadCpuDuration is the time spent downloading; downloadedSize is the compressed artifact size.
    TCallback<void(TCpuDuration downloadCpuDuration, i64 downloadedSize)> OnLayerDownloaded;

    //! Called after a layer artifact is successfully imported into Porto.
    TCallback<void(TCpuDuration importCpuDuration, i64 importedSize)> OnLayerImported;
};

////////////////////////////////////////////////////////////////////////////////

struct TArtifactKey
    : public NProto::TArtifactKey
{
    TArtifactKey(
        NChunkClient::EDataSourceType dataSource,
        std::vector<NChunkClient::NProto::TChunkSpec> specs);

    explicit TArtifactKey(const NProto::TArtifactKey& key);
    explicit TArtifactKey(const NControllerAgent::NProto::TFileDescriptor& descriptor);

    i64 GetCompressedDataSize() const;
    std::optional<i64> TryGetFileSizeEstimate() const;
    i64 GetFileSizeEstimateOrCrash() const;

    //! Hasher.
    operator size_t() const;

    //! Comparer.
    bool operator==(const TArtifactKey& other) const;

};

void FormatValue(TStringBuilderBase* builder, const TArtifactKey& key, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
