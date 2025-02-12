#pragma once

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TReqFetch;

class TReqExportChunks;
class TRspExportChunks;

class TReqImportChunks;
class TRspImportChunks;

class TReqExecuteBatch;
class TRspExecuteBatch;

class TDataSource;
class TDataSourceDirectoryExt;

class TDataSink;
class TDataSinkDirectoryExt;

class TReqGetChunkMeta;

class TAllyReplicasInfo;
class TChunkReplicaAnnouncement;
class TChunkReplicaAnnouncementRequest;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TLoadContext;
using NTableClient::TSaveContext;
using NTableClient::TPersistenceContext;

constexpr int MaxMasterChunkMetaExtensions = 6;

struct TBlock;

using TMediumId = NObjectClient::TObjectId;

using TReadSessionId = NObjectClient::TObjectId;

struct TSessionId;

constexpr NRpc::TRealmId ProxyingDataNodeServiceRealmId = TGuid(0xd452d72f, 0x3142caa3);

constexpr int DefaultPartIndex = -1;

//! Estimated memory overhead per chunk reader.
constexpr i64 ChunkReaderMemorySize = 16_KB;

constexpr int MaxMediumPriority = 10;

constexpr i64 DefaultMaxBlockSize = 16_MB;
constexpr int MaxInputChunkReplicaCount = 16;

//! Represents an offset inside a chunk.
using TBlockOffset = i64;

//! A |(chunkId, blockIndex)| pair.
struct TBlockId;

using TConsistentReplicaPlacementHash = ui64;
constexpr TConsistentReplicaPlacementHash NullConsistentReplicaPlacementHash = 0;

//! All chunks are uniformly divided into |ChunkShardCount| shards.
// BEWARE: Changing this value requires reign promotion since rolling update
// is not possible.
constexpr int ChunkShardCount = 60;
static_assert(ChunkShardCount < std::numeric_limits<i8>::max(), "ChunkShardCount must fit into i8");

//! Typical chunk location count per data node.
constexpr int TypicalChunkLocationCount = 20;

struct TAllyReplicasInfo;

constexpr int WholeBlockFragmentRequestLength = -1;

DEFINE_BIT_ENUM(EBlockType,
    ((None)                        (0x0000))
    //! This basically comprises any block regardless of its semantics (data or some system block).
    ((CompressedData)              (0x0001))
    //! Uncompressed data block.
    ((UncompressedData)            (0x0002))
    //! Hash table chunk index system block.
    ((HashTableChunkIndex)         (0x0004))
    //! Xor filter system block.
    ((XorFilter)                   (0x0008))
    //! Blocks used by chunk fragment reader cache.
    ((ChunkFragmentsData)          (0x0010))
);

DEFINE_ENUM(EChunkType,
    ((Unknown) (0))
    ((File)    (1))
    ((Table)   (2))
    ((Journal) (3))
    ((Hunk)    (4))
);

//! Values must be contiguous.
DEFINE_ENUM(ESessionType,
    ((User)                     (0))
    ((Replication)              (1))
    ((Repair)                   (2))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EChunkFeatures, ui64,
    ((None)                     (0x00000000))
    ((DescendingSortOrder)      (0x00000001))
    ((StripedErasure)           (0x00000002))
    ((IndexedBlockFormat)       (0x00000004))
    ((SlimBlockFormat)          (0x00000008))
    ((UnversionedHunks)         (0x00000010))
    ((CompressedHunkValues)     (0x00000020))
    ((NoColumnMetaInChunkMeta)  (0x00000040))
    // Sentinel
    ((Unknown)                   (0x80000000))
);

DEFINE_ENUM_UNKNOWN_VALUE(EChunkFeatures, Unknown);

DEFINE_ENUM(EChunkClientFeature,
    // COMPAT(akozhikhov).
    ((AllBlocksIndex)           (0))
);

// TODO(cherepashka): remove after corresponding compat in 25.1 will be removed.
DEFINE_ENUM(ECompatChunkMergerMode,
    ((None)         (0))
    ((Shallow)      (1))
    ((Deep)         (2))
    ((Auto)         (3))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EChunkMergerMode, i8,
    ((None)         (0))
    ((Shallow)      (1))
    ((Deep)         (2))
    ((Auto)         (3))
);

DEFINE_ENUM(EChunkListContentType,
    ((Main)                   (0))
    ((Hunk)                   (1))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRemoteReaderOptions)
DECLARE_REFCOUNTED_CLASS(TDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TDispatcherDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterOptions)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteWriterOptions)
DECLARE_REFCOUNTED_CLASS(TMetaAggregatingWriterOptions)
DECLARE_REFCOUNTED_CLASS(TBlockCacheConfig)
DECLARE_REFCOUNTED_CLASS(TBlockCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TClientChunkMetaCacheConfig)
DECLARE_REFCOUNTED_CLASS(TChunkScraperConfig)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporterConfig)
DECLARE_REFCOUNTED_CLASS(TMediumDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TChunkReplicaCacheConfig)

DECLARE_REFCOUNTED_STRUCT(IFetcherChunkScraper)

DECLARE_REFCOUNTED_CLASS(TEncodingWriter)
DECLARE_REFCOUNTED_CLASS(TEncodingChunkWriter)
DECLARE_REFCOUNTED_CLASS(TBlockFetcher)
DECLARE_REFCOUNTED_CLASS(TSequentialBlockFetcher)

DECLARE_REFCOUNTED_STRUCT(IChunkReader)
DECLARE_REFCOUNTED_STRUCT(IChunkFragmentReader)
DECLARE_REFCOUNTED_STRUCT(IChunkReaderAllowingRepair)

DECLARE_REFCOUNTED_STRUCT(IReaderBase)
DECLARE_REFCOUNTED_STRUCT(IReaderFactory)

DECLARE_REFCOUNTED_STRUCT(IMultiReaderManager)

DECLARE_REFCOUNTED_CLASS(TTrafficMeter)

DECLARE_REFCOUNTED_STRUCT(IChunkWriterBase)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IMetaAggregatingWriter)

DECLARE_REFCOUNTED_STRUCT(IBlockCache)
DECLARE_REFCOUNTED_STRUCT(IClientBlockCache)

DECLARE_REFCOUNTED_CLASS(TMemoryWriter)

DECLARE_REFCOUNTED_CLASS(TInputChunk)
DECLARE_REFCOUNTED_CLASS(TInputChunkSlice)
DECLARE_REFCOUNTED_CLASS(TWeightedInputChunk)

DECLARE_REFCOUNTED_STRUCT(TLegacyDataSlice)

DECLARE_REFCOUNTED_CLASS(TDataSourceDirectory)
DECLARE_REFCOUNTED_CLASS(TDataSinkDirectory)

DECLARE_REFCOUNTED_CLASS(TChunkScraper)
DECLARE_REFCOUNTED_CLASS(TScraperTask)
DECLARE_REFCOUNTED_CLASS(TThrottlerManager)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporter)
DECLARE_REFCOUNTED_CLASS(TMediumDirectory)
DECLARE_REFCOUNTED_CLASS(TMediumDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TChunkMetaFetcher)

DECLARE_REFCOUNTED_CLASS(TMasterChunkSpecFetcher)
DECLARE_REFCOUNTED_CLASS(TTabletChunkSpecFetcher)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderStatistics)
DECLARE_REFCOUNTED_STRUCT(TChunkWriterStatistics)

DECLARE_REFCOUNTED_STRUCT(IReaderMemoryManager)
DECLARE_REFCOUNTED_CLASS(TChunkReaderMemoryManager)

DECLARE_REFCOUNTED_STRUCT(IChunkReplicaCache)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderHost)

struct TChunkReaderMemoryManagerOptions;

struct TUserObject;

using TRefCountedChunkMeta = TRefCountedProto<NChunkClient::NProto::TChunkMeta>;
DECLARE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TDeferredChunkMeta)

DECLARE_REFCOUNTED_CLASS(TMemoryTrackedDeferredChunkMeta)

// NB: TRefCountedBlocksExt needs weak pointers support.
using TRefCountedBlocksExt = TRefCountedProto<NChunkClient::NProto::TBlocksExt>;
DECLARE_REFCOUNTED_TYPE(TRefCountedBlocksExt)

using TRefCountedMiscExt = TRefCountedProto<NChunkClient::NProto::TMiscExt>;
DECLARE_REFCOUNTED_TYPE(TRefCountedMiscExt)

using TPlacementId = TGuid;

struct TDataSliceDescriptor;

struct TInterruptDescriptor;

class TCodecStatistics;

struct TClientChunkReadOptions;

using TDataCenterName = std::optional<std::string>;

DECLARE_REFCOUNTED_CLASS(TMemoryUsageGuard)

DECLARE_REFCOUNTED_CLASS(TChunkReaderMemoryManagerHolder)

DECLARE_REFCOUNTED_STRUCT(IMultiReaderMemoryManager)
DECLARE_REFCOUNTED_STRUCT(IReaderMemoryManagerHost)

DECLARE_REFCOUNTED_STRUCT(ICachedChunkMeta)
DECLARE_REFCOUNTED_STRUCT(IClientChunkMetaCache)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TDispatcherConfig, TDispatcherDynamicConfig);

class TDataSink;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
