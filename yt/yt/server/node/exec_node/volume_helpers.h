#pragma once

#include "private.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TBaseVolumeParams
    : public TRefCounted
{
public:
    const std::string VolumeId;
    const EVolumeType VolumeType;

    //! Slot user id.
    const int UserId = 0;

    i64 Size = 0;
    std::vector<TArtifactKey> LayerArtifactKeys;

    //! If true, the volume can be reused between sequential jobs within the same allocation.
    bool AllowReusing = false;

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

} // namespace NYT::NExecNode
