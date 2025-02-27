#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/misc/config.h>
#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TRemoteReaderOptions
    : public virtual NYTree::TYsonStruct
{
public:
    //! If |true| then the master may be asked for seeds.
    bool AllowFetchingSeedsFromMaster;

    //! Advertise current host as a P2P peer.
    bool EnableP2P;

    bool UseProxyingDataNodeService;

    REGISTER_YSON_STRUCT(TRemoteReaderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteWriterOptions
    : public virtual TMemoryTrackedWriterOptions
{
public:
    bool AllowAllocatingNewTargetNodes;
    TString MediumName;
    TPlacementId PlacementId;

    REGISTER_YSON_STRUCT(TRemoteWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteWriterOptions)

////////////////////////////////////////////////////////////////////////////////

struct TDispatcherDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<int> ChunkReaderPoolSize;

    REGISTER_YSON_STRUCT(TDispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDispatcherConfig
    : public virtual NYTree::TYsonStruct
{
    static constexpr int DefaultChunkReaderPoolSize = 8;
    int ChunkReaderPoolSize;

    TDispatcherConfigPtr ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterOptions
    : public virtual TEncodingWriterOptions
    , public virtual TRemoteWriterOptions
{
public:
    static constexpr int InvalidTableIndex = -1;

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
    bool EnableStripedErasure;

    //! Table and chunk schema might differ. By default they are assumed
    //! to be equal, this value overrides table schema, if set. Table schema
    //! cannot be stricter than chunk schema.
    NTableClient::TTableSchemaPtr TableSchema;

    NChunkClient::TConsistentReplicaPlacementHash ConsistentChunkReplicaPlacementHash;

    REGISTER_YSON_STRUCT(TMultiChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderOptions
    : public TRemoteReaderOptions
{
public:
    bool KeepInMemory;

    REGISTER_YSON_STRUCT(TMultiChunkReaderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TMetaAggregatingWriterOptions
    : public TMultiChunkWriterOptions
{
public:
    bool EnableSkynetSharing;
    int MaxHeavyColumns;
    bool AllowUnknownExtensions;
    std::optional<i64> MaxBlockCount;

    REGISTER_YSON_STRUCT(TMetaAggregatingWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMetaAggregatingWriterOptions)

////////////////////////////////////////////////////////////////////////////////

struct TBlockCacheConfig
    : public virtual NYTree::TYsonStruct
{
    // TODO(akozhikhov): Use enumeration instead (same for dynamic config).
    TSlruCacheConfigPtr CompressedData;
    TSlruCacheConfigPtr UncompressedData;
    TSlruCacheConfigPtr HashTableChunkIndex;
    TSlruCacheConfigPtr XorFilter;
    TSlruCacheConfigPtr ChunkFragmentsData;

    REGISTER_YSON_STRUCT(TBlockCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientChunkMetaCacheConfig
    : public TSlruCacheConfig
{
    REGISTER_YSON_STRUCT(TClientChunkMetaCacheConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TClientChunkMetaCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBlockCacheDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    TSlruCacheDynamicConfigPtr CompressedData;
    TSlruCacheDynamicConfigPtr UncompressedData;
    TSlruCacheDynamicConfigPtr HashTableChunkIndex;
    TSlruCacheDynamicConfigPtr XorFilter;
    TSlruCacheDynamicConfigPtr ChunkFragmentsData;

    REGISTER_YSON_STRUCT(TBlockCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkScraperConfig
    : public virtual NYTree::TYsonStruct
{
    //! Number of chunks scratched per one LocateChunks.
    int MaxChunksPerRequest;

    REGISTER_YSON_STRUCT(TChunkScraperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkTeleporterConfig
    : public virtual NYTree::TYsonStruct
{
    //! Maximum number of chunks to export/import per request.
    i64 MaxTeleportChunksPerRequest;

    REGISTER_YSON_STRUCT(TChunkTeleporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMediumDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    REGISTER_YSON_STRUCT(TMediumDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaCacheConfig
    : public virtual NYTree::TYsonStruct
{
    TDuration ExpirationTime;
    TDuration ExpirationSweepPeriod;
    int MaxChunksPerLocate;

    REGISTER_YSON_STRUCT(TChunkReplicaCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicaCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
