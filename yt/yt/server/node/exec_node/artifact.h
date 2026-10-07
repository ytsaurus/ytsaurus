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

struct TArtifactDownloadOptions
{
    NChunkClient::TTrafficMeterPtr TrafficMeter;
    std::vector<std::string> WorkloadDescriptorAnnotations;

    //! Called after a layer artifact is downloaded (and imported, for Porto layers).
    //! Parameters: downloadCpuDuration (network fetch), importCpuDuration (Porto import; zero for SquashFS),
    //!             importSize (compressed archive size of the imported layer; zero for SquashFS).
    TCallback<void(TCpuDuration downloadCpuDuration, TCpuDuration importCpuDuration, i64 importSize)> OnLayerDownloaded;
};

////////////////////////////////////////////////////////////////////////////////

struct TArtifactKey
    : public NProto::TArtifactKey
{
    TArtifactKey(
        NChunkClient::EDataSourceType dataSource,
        const std::vector<NChunkClient::NProto::TChunkSpec>& specs);

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
