#pragma once

#include "private.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/library/nbd/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TChunkNbdVolumeSpec
{
    int MediumIndex = 0;

    //! Params to connect to chosen data nodes.
    TDuration DataNodeRpcTimeout;
    std::optional<std::string> DataNodeAddress;

    //! Params for NBD requests to data nodes.
    TDuration DataNodeNbdServiceRpcTimeout;
    TDuration DataNodeNbdServiceMakeTimeout;

    //! Params to get suitable data nodes from master.
    TDuration MasterRpcTimeout;
    int MinDataNodeCount = 0;
    int MaxDataNodeCount = 0;

    //! Number of TCP connections to use for NBD RPC requests.
    int MultiplexingParallelism = DefaultNbdMultiplexingParallelism;

    bool operator==(const TChunkNbdVolumeSpec&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TChunkNbdVolumeSpec& volumeSpec, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

using TRWNbdVolumeBackendSpec = std::variant<TChunkNbdVolumeSpec>;

//! Sandbox NBD root volume as requested by the job spec.
struct TSandboxNbdRootVolumeSpec
{
    //! Identifier of NBD disk within NBD server.
    std::string DeviceId;

    //! Volume params.
    i64 DeviceSize = 0;
    NNbd::EFilesystemType FilesystemType = NNbd::EFilesystemType::Ext4;

    TRWNbdVolumeBackendSpec BackendSpec;

    bool operator==(const TSandboxNbdRootVolumeSpec&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeSpec& volumeSpec, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

class TBaseVolumeParams
    : public TRefCounted
{
public:
    class TLayerArtifactKeysStorage
    {
    public:
        TLayerArtifactKeysStorage() = default;
        explicit TLayerArtifactKeysStorage(const TLayerArtifactKeysStorage&) = default;

        // NB: Only the regular keys take part in comparison. Volatile keys (e.g. GPU topping layers)
        // are re-derived per job and may legitimately differ within one allocation.
        bool operator==(const TLayerArtifactKeysStorage& other) const;

        std::span<TArtifactKey> GetAll();
        std::span<const TArtifactKey> GetAll() const;
        void AddRegularArtifactKeys(std::vector<TArtifactKey> regularArtifactKeys);
        void SetVolatileArtifactKeys(std::vector<TArtifactKey> volatileArtifactKeys);

    private:
        // MergedArtifactKeys_ is partitioned into volatile and regular layer artifact keys.
        // [0, VolatileArtifactKeyCount_) contains volatile keys,
        // [VolatileArtifactKeyCount_, size()) contains regular keys.
        std::vector<TArtifactKey> MergedArtifactKeys_;
        int VolatileArtifactKeyCount_ = 0;
    };

public:
    const std::string VolumeId;
    const EVolumeType VolumeType;

    //! Slot user id.
    const int UserId = 0;

    std::optional<i64> Size;

    TLayerArtifactKeysStorage LayerArtifactKeys;

    //! If true, the volume can be reused between sequential jobs within the same allocation.
    bool AllowReusing = false;

    explicit TBaseVolumeParams(const TBaseVolumeParams& volumeParams) = default;
    TBaseVolumeParams(std::string volumeId, EVolumeType volumeType, int userId);

    bool operator==(const TBaseVolumeParams& other) const;

    virtual void Format(TStringBuilderBase* builder) const;

    virtual ~TBaseVolumeParams() = default;
};

DECLARE_REFCOUNTED_CLASS(TBaseVolumeParams)
DEFINE_REFCOUNTED_TYPE(TBaseVolumeParams)

void FormatValue(TStringBuilderBase* builder, const TBaseVolumeParamsPtr& params, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

class TTmpfsVolumeParams
    : public TBaseVolumeParams
{
public:
    // COMPAT(krasovav)
    int Index = 0;

    explicit TTmpfsVolumeParams(const TTmpfsVolumeParams& volumeParams) = default;
    TTmpfsVolumeParams(std::string volumeId, int userId);

    void Format(TStringBuilderBase* builder) const override;

    // NB: We explicitly compare only derived class members here.
    // Base class comparison is handled by TBaseVolumeParams::operator==
    // which dispatches to this method. Using = default would cause infinite recursion.
    bool operator==(const TTmpfsVolumeParams& other) const;
};

DECLARE_REFCOUNTED_CLASS(TTmpfsVolumeParams)
DEFINE_REFCOUNTED_TYPE(TTmpfsVolumeParams)

////////////////////////////////////////////////////////////////////////////////

class TLocalDiskVolumeParams
    : public TBaseVolumeParams
{
public:
    std::optional<i64> InodeLimit;

    explicit TLocalDiskVolumeParams(const TLocalDiskVolumeParams& volumeParams) = default;
    TLocalDiskVolumeParams(std::string volumeId, int userId);

    void Format(TStringBuilderBase* builder) const override;

    // NB: We explicitly compare only derived class members here.
    // Base class comparison is handled by TBaseVolumeParams::operator==
    // which dispatches to this method. Using = default would cause infinite recursion.
    bool operator==(const TLocalDiskVolumeParams& other) const;
};

DECLARE_REFCOUNTED_CLASS(TLocalDiskVolumeParams)
DEFINE_REFCOUNTED_TYPE(TLocalDiskVolumeParams)

////////////////////////////////////////////////////////////////////////////////

class TNbdDiskVolumeParams
    : public TBaseVolumeParams
{
public:
    TSandboxNbdRootVolumeSpec SandboxNbdRootVolumeSpec;

    TNbdDiskVolumeParams(
        std::string volumeId,
        int userId,
        TSandboxNbdRootVolumeSpec sandboxNbdRootVolumeSpec);
    explicit TNbdDiskVolumeParams(const TNbdDiskVolumeParams& volumeParams) = default;

    void Format(TStringBuilderBase* builder) const override;

    bool operator==(const TNbdDiskVolumeParams& other) const;
};

DECLARE_REFCOUNTED_CLASS(TNbdDiskVolumeParams)
DEFINE_REFCOUNTED_TYPE(TNbdDiskVolumeParams)

////////////////////////////////////////////////////////////////////////////////

class TVolumeResult
    : public TRefCounted
{
public:
    TVolumeResult(std::string volumeId, EVolumeType volumeType, IVolumePtr&& Volume);

    std::string VolumeId;

    EVolumeType VolumeType;

    IVolumePtr Volume;
};

DECLARE_REFCOUNTED_CLASS(TVolumeResult)
DEFINE_REFCOUNTED_TYPE(TVolumeResult)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(krasovav)
class TTmpfsVolumeResult
    : public TVolumeResult
{
public:
    TTmpfsVolumeResult(std::string volumeId, EVolumeType volumeType, IVolumePtr&& volume, int index);

    int Index = 0;
};

DECLARE_REFCOUNTED_CLASS(TTmpfsVolumeResult)
DEFINE_REFCOUNTED_TYPE(TTmpfsVolumeResult)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TVolumeMount& volumeMount, NYT::NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
