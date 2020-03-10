#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
class TClusterResources
{
public:
    TClusterResources();

    //! Get infinite resources.
    static TClusterResources Infinite();

    //! Set node count.
    TClusterResources&& SetNodeCount(i64 nodeCount) &&;

    //! Set chunk count.
    TClusterResources&& SetChunkCount(i64 chunkCount) &&;

    //! Set tablet count.
    TClusterResources&& SetTabletCount(int tabletCount) &&;

    //! Set tablet static memory size.
    TClusterResources&& SetTabletStaticMemory(i64 tabletStaticMemory) &&;

    TClusterResources&& SetMasterMemory(i64 masterMemory) &&;

    //! Set medium disk space.
    TClusterResources&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    //! Completely empties disk space counts for all media.
    void ClearDiskSpace();

    bool IsAtLeastOneResourceLessThan(const TClusterResources& rhs) const;

private:
    //! Space occupied on data nodes in bytes per medium.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     *
     *  Zero disk space for a medium is considered equivalent to that medium
     *  missing an entry in this map. In particular, setting zero disk space for
     *  a medium leads to erasing it from the map altogether.
     */
    NChunkClient::TMediumMap<i64> DiskSpace_;

public:
    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    //! Number of Cypress nodes created at master.
    /*!
     *  Branched copies are also counted.
     */
    i64 NodeCount;

    //! Number of chunks created at master.
    i64 ChunkCount;

    //! Number of tablets.
    int TabletCount;

    //! Occupied tablet static memory.
    i64 TabletStaticMemory;

    //! Occupied master memory.
    i64 MasterMemory;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);
};

////////////////////////////////////////////////////////////////////////////////

//! A helper for (de)serializing TClusterResources.
//! This cannot be done directly as serialization requires converting medium
//! indexes to names, which is impossible without the chunk manager.
class TSerializableClusterResources
    : public NYTree::TYsonSerializable
{
public:
    // For deserialization.
    explicit TSerializableClusterResources(bool serializeDiskSpace = true);
    // For serialization.
    TSerializableClusterResources(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const TClusterResources& clusterResources,
        bool serializeDiskSpace = true);

    TClusterResources ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const;

    void AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace);

private:
    i64 NodeCount_ = 0;
    i64 ChunkCount_ = 0;
    int TabletCount_ = 0;
    i64 TabletStaticMemory_ = 0;
    THashMap<TString, i64> DiskSpacePerMedium_;
    i64 DiskSpace_; // Compatibility.
    i64 MasterMemory_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TSerializableClusterResources)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources);
void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources);

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator +  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator -  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs);
TClusterResources  operator *  (const TClusterResources& lhs, i64 rhs);

TClusterResources  operator -  (const TClusterResources& resources);

bool operator == (const TClusterResources& lhs, const TClusterResources& rhs);
bool operator != (const TClusterResources& lhs, const TClusterResources& rhs);

void FormatValue(TStringBuilderBase* builder, const TClusterResources& resources, TStringBuf /*format*/);
TString ToString(const TClusterResources& resources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

