#pragma once

#include "public.h"
#include "master_memory.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/client/cell_master_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterResourceLimits
{
public:
    static TClusterResourceLimits Infinite();
    static TClusterResourceLimits Zero(const NCellMaster::IMulticellManagerPtr& multicellManager);

    TClusterResourceLimits&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    bool IsViolatedBy(const TClusterResourceLimits& rhs) const;

    TViolatedClusterResourceLimits GetViolatedBy(const TClusterResourceLimits& usage) const;

    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, i64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, i64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, int, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, i64, TabletStaticMemory);

    TMasterMemoryLimits& MasterMemory();
    const TMasterMemoryLimits& MasterMemory() const;
    TClusterResourceLimits&& SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &&;
    void SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &;

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    TClusterResourceLimits& operator += (const TClusterResourceLimits& other);
    TClusterResourceLimits operator + (const TClusterResourceLimits& other) const;

    TClusterResourceLimits& operator -= (const TClusterResourceLimits& other);
    TClusterResourceLimits operator - (const TClusterResourceLimits& other) const;

    TClusterResourceLimits& operator *= (i64 other);
    TClusterResourceLimits operator * (i64 other) const;

    TClusterResourceLimits operator - () const;

    bool operator == (const TClusterResourceLimits& other) const;

private:
    NChunkClient::TMediumMap<i64> DiskSpace_;
    TMasterMemoryLimits MasterMemory_;
};

////////////////////////////////////////////////////////////////////////////////

class TViolatedClusterResourceLimits
{
public:
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, int, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, TabletStaticMemory);

    TMasterMemoryLimits& MasterMemory();
    const TMasterMemoryLimits& MasterMemory() const;
    void SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &;

    NChunkClient::TMediumMap<i64>& DiskSpace();
    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

private:
    NChunkClient::TMediumMap<i64> DiskSpace_;
    TMasterMemoryLimits MasterMemory_;
};

// NB: this serialization requires access to chunk and multicell managers and
// cannot be easily integrated into yson serialization framework.

void SerializeClusterResourceLimits(
    const TClusterResourceLimits& resourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace);

void DeserializeClusterResourceLimits(
    TClusterResourceLimits& resourceLimits,
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeViolatedClusterResourceLimits(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeViolatedClusterResourceLimitsInCompactFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeViolatedClusterResourceLimitsInBooleanFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TClusterResourceLimits& resources, TStringBuf /*format*/);
TString ToString(const TClusterResourceLimits& resources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

