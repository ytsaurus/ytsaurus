#pragma once

#include "public.h"

#include <yt/client/misc/config.h>
#include <yt/client/misc/workload.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/api/public.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/client/chunk_client/config.h>

#include <yt/core/compression/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/config.h>
#include <yt/core/misc/error.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TRemoteReaderOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    //! If |true| then the master may be asked for seeds.
    bool AllowFetchingSeedsFromMaster;

    //! Advertise current host as a P2P peer.
    bool EnableP2P;

    TRemoteReaderOptions()
    {
        RegisterParameter("allow_fetching_seeds_from_master", AllowFetchingSeedsFromMaster)
            .Default(true);

        RegisterParameter("enable_p2p", EnableP2P)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteWriterOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    bool AllowAllocatingNewTargetNodes;
    TString MediumName;
    TPlacementId PlacementId;

    TRemoteWriterOptions()
    {
        RegisterParameter("allow_allocating_new_target_nodes", AllowAllocatingNewTargetNodes)
            .Default(true);
        RegisterParameter("medium_name", MediumName)
            .Default(DefaultStoreMediumName);
        RegisterParameter("placement_id", PlacementId)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    NCompression::ECodec CompressionCodec;
    bool ChunksEden;

    TEncodingWriterOptions()
    {
        RegisterParameter("compression_codec", CompressionCodec)
            .Default(NCompression::ECodec::None);
        RegisterParameter("chunks_eden", ChunksEden)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TDispatcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    int CompressionPoolSize;
    int ErasurePoolSize;

    TDispatcherConfig()
    {
        RegisterParameter("compression_pool_size", CompressionPoolSize)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("erasure_pool_size", ErasurePoolSize)
            .Default(4)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterOptions
    : public virtual TEncodingWriterOptions
    , public virtual TRemoteWriterOptions
{
public:
    int ReplicationFactor;
    TString Account;
    bool ChunksVital;
    bool ChunksMovable;
    bool ValidateResourceUsageIncrease;

    //! This field doesn't affect the behavior of writer.
    //! It is stored in table_index field of output_chunk_specs.
    int TableIndex;

    NErasure::ECodec ErasureCodec;

    TMultiChunkWriterOptions()
    {
        RegisterParameter("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(DefaultReplicationFactor);
        RegisterParameter("account", Account)
            .NonEmpty();
        RegisterParameter("chunks_vital", ChunksVital)
            .Default(true);
        RegisterParameter("chunks_movable", ChunksMovable)
            .Default(true);
        RegisterParameter("validate_resource_usage_increase", ValidateResourceUsageIncrease)
            .Default(true);
        RegisterParameter("erasure_codec", ErasureCodec)
            .Default(NErasure::ECodec::None);
        RegisterParameter("table_index", TableIndex)
            .Default(-1);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderOptions
    : public TRemoteReaderOptions
{
public:
    bool KeepInMemory;

    TMultiChunkReaderOptions()
    {
        RegisterParameter("keep_in_memory", KeepInMemory)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TBlockCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CompressedData;
    TSlruCacheConfigPtr UncompressedData;

    i64 GetTotalCapacity() const
    {
        return
            CompressedData->Capacity +
            UncompressedData->Capacity;
    }

    TBlockCacheConfig()
    {
        RegisterParameter("compressed_data", CompressedData)
            .DefaultNew();
        RegisterParameter("uncompressed_data", UncompressedData)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkScraperConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Number of chunks scratched per one LocateChunks.
    int MaxChunksPerRequest;

    TChunkScraperConfig()
    {
        RegisterParameter("max_chunks_per_request", MaxChunksPerRequest)
            .Default(10000)
            .GreaterThan(0)
            .LessThan(100000);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkTeleporterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Maximum number of chunks to export/import per request.
    int MaxTeleportChunksPerRequest;

    TChunkTeleporterConfig()
    {
        RegisterParameter("max_teleport_chunks_per_request", MaxTeleportChunksPerRequest)
            .GreaterThan(0)
            .Default(5000);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
