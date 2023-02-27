#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/aggregate_property.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatisticsBase
{
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 HunkUncompressedDataSize = 0;
    i64 HunkCompressedDataSize = 0;
    i64 MemorySize = 0;
    i64 DynamicMemoryPoolSize = 0;
    NChunkClient::TCompactMediumMap<i64> DiskSpacePerMedium;
    int ChunkCount = 0;
    int PartitionCount = 0;
    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;
    int TabletCount = 0;
    TEnumIndexedVector<NTabletClient::EInMemoryMode, int> TabletCountPerMemoryMode;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTabletCellStatistics
    : public TTabletCellStatisticsBase
{
    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTabletStatisticsBase
{
    int OverlappingStoreCount = 0;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTabletStatistics
    : public TTabletCellStatisticsBase
    , public TTabletStatisticsBase
{
    void Persist(const NCellMaster::TPersistenceContext& context);
};

class TTabletStatisticsAggregate
    : public TNonCopyable
{
public:
    TTabletStatistics Get() const;

    void Account(const TTabletStatistics& tabletStatistics);
    void Discount(const TTabletStatistics& tabletStatistics);
    void AccountDelta(const TTabletStatistics& tabletStatistics);

    void Reset();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

private:
    TSumAggregate<TTabletStatistics> CellStatistics_;
    TMaxAggregate<int> OverlappingStoreCount_{0};
};

////////////////////////////////////////////////////////////////////////////////

TTabletCellStatisticsBase& operator += (TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);
TTabletCellStatisticsBase  operator +  (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

TTabletCellStatisticsBase& operator -= (TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);
TTabletCellStatisticsBase  operator -  (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

bool operator == (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);
bool operator != (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

TTabletCellStatistics& operator += (TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);
TTabletCellStatistics  operator +  (const TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);

TTabletCellStatistics& operator -= (TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);
TTabletCellStatistics  operator -  (const TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);

TTabletStatistics& operator += (TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

TTabletStatistics& operator += (TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

bool operator == (const TTabletStatistics& lhs, const TTabletStatistics& rhs);
bool operator != (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

void ToProto(NProto::TTabletCellStatistics* protoStatistics, const TTabletCellStatistics& statistics);
void FromProto(TTabletCellStatistics* statistics, const NProto::TTabletCellStatistics& protoStatistics);

TString ToString(const TTabletStatistics& statistics, const NChunkServer::IChunkManagerPtr& chunkManager);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(akozhikhov)
class TSerializableTabletCellStatisticsBase
    : public virtual NYTree::TYsonSerializable
    , public TTabletCellStatisticsBase
{
public:
    TSerializableTabletCellStatisticsBase();

    TSerializableTabletCellStatisticsBase(
        const TTabletCellStatisticsBase& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);

private:
    i64 DiskSpace_ = 0;
    THashMap<TString, i64> DiskSpacePerMediumMap_;

    void InitParameters();
};

class TSerializableTabletStatisticsBase
    : public virtual NYTree::TYsonSerializable
    , public TTabletStatisticsBase
{
public:
    TSerializableTabletStatisticsBase();

    explicit TSerializableTabletStatisticsBase(const TTabletStatisticsBase& statistics);

private:
    void InitParameters();
};

class TSerializableTabletCellStatistics
    : public TSerializableTabletCellStatisticsBase
{
public:
    TSerializableTabletCellStatistics() = default;

    TSerializableTabletCellStatistics(
        const TTabletCellStatistics& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);
};

class TSerializableTabletStatistics
    : public TSerializableTabletCellStatisticsBase
    , public TSerializableTabletStatisticsBase
{
public:
    TSerializableTabletStatistics() = default;

    TSerializableTabletStatistics(
        const TTabletStatistics& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
