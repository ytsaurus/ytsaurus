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

////////////////////////////////////////////////////////////////////////////////

struct TArtifactDownloadContext
{
    NChunkClient::TTrafficMeterPtr TrafficMeter;
    std::vector<std::string> WorkloadDescriptorAnnotations;

    //! Called when a layer is served from the layer volume cache (tmpfs, SLRU or SquashFS volume cache)
    //! instead of being fetched over the network.
    TCallback<void(i64 compressedDataSize)> OnLayerCacheHit;

    //! Called when a layer is served from the local chunk cache instead of being fetched over the network.
    //! NB: in rare fallback cases (see OnLayerDownloaded) this callback fires optimistically
    //! even though the layer is re-downloaded over the network.
    TCallback<void(i64 compressedDataSize)> OnChunkCacheHit;

    //! Called when a layer was awaited while another job was inserting it into the layer volume cache.
    //! Parameters: asyncHitCpuDuration (time the concurrent insertion was awaited), compressedDataSize.
    TCallback<void(TCpuDuration asyncHitCpuDuration, i64 compressedDataSize)> OnLayerCacheAsyncHit;

    //! Called when a layer was awaited while another job was fetching it into the chunk cache.
    //! Parameters: asyncHitCpuDuration (time the concurrent fetch was awaited), compressedDataSize.
    TCallback<void(TCpuDuration asyncHitCpuDuration, i64 compressedDataSize)> OnChunkCacheAsyncHit;

    //! Called after a layer artifact is fetched over the network; skipped when it is served locally.
    //! NB: rare fallbacks (a corrupted registered chunk, an empty pending registration) re-download
    //! over the network while the event is still Hit: OnChunkCacheHit fires instead
    //! of this callback, and the network bytes go unaccounted.
    //! Parameters: downloadCpuDuration, downloadedSize (compressed artifact size).
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
