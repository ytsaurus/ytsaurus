#pragma once

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/small_vector.h>
#include <yt/yt/core/misc/optional.h>

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

class TReqGetChunkMeta;

class TChunkReplicaAnnouncement;
class TChunkReplicaAnnouncementRequest;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxMasterChunkMetaExtensions = 6;

struct TBlock;

constexpr int AllBlocksIndex = -1;

using TMediumId = NObjectClient::TObjectId;

using TReadSessionId = NObjectClient::TObjectId;

struct TSessionId;

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

DEFINE_BIT_ENUM(EBlockType,
    ((None)              (0x0000))
    ((CompressedData)    (0x0001))
    ((UncompressedData)  (0x0002))
);

DEFINE_ENUM(EChunkType,
    ((Unknown) (0))
    ((File)    (1))
    ((Table)   (2))
    ((Journal) (3))
    ((Hunk)    (4))
);

// Keep in sync with NChunkServer::ETableChunkFormat.
DEFINE_ENUM(EChunkFormat,
    // Sentinels.
    ((Unknown)                   (-1))
    // File chunks.
    ((FileDefault)                (1))
    // Table chunks.
    ((TableVersionedSimple)       (2))
    ((TableSchemaful)             (3))
    ((TableSchemalessHorizontal)  (4))
    ((TableVersionedColumnar)     (5))
    ((TableUnversionedColumnar)   (6))
    // Journal chunks.
    ((JournalDefault)             (0))
    // Hunk chunks.
    ((HunkDefault)                (7))
);

//! Values must be contiguous.
DEFINE_ENUM(ESessionType,
    ((User)                     (0))
    ((Replication)              (1))
    ((Repair)                   (2))
);

DEFINE_ENUM(EUpdateMode,
    ((None)                     (0))
    ((Append)                   (1))
    ((Overwrite)                (2))
);

DEFINE_BIT_ENUM(EChunkFeatures,
    ((None)                     (0x0000))
    ((DescendingSortOrder)      (0x0001))
);

DEFINE_ENUM(EChunkClientFeature,
    ((AllBlocksIndex)           (0))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRemoteReaderOptions)
DECLARE_REFCOUNTED_CLASS(TDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TDispatcherDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterOptions)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteWriterOptions)
DECLARE_REFCOUNTED_CLASS(TBlockCacheConfig)
DECLARE_REFCOUNTED_CLASS(TBlockCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TClientChunkMetaCacheConfig)
DECLARE_REFCOUNTED_CLASS(TChunkScraperConfig)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporterConfig)
DECLARE_REFCOUNTED_CLASS(TMediumDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TChunkFragmentReaderConfig)

DECLARE_REFCOUNTED_STRUCT(IFetcherChunkScraper)

DECLARE_REFCOUNTED_CLASS(TEncodingWriter)
DECLARE_REFCOUNTED_CLASS(TEncodingChunkWriter)
DECLARE_REFCOUNTED_CLASS(TBlockFetcher)
DECLARE_REFCOUNTED_CLASS(TSequentialBlockFetcher)

DECLARE_REFCOUNTED_STRUCT(IChunkReader)
DECLARE_REFCOUNTED_STRUCT(IChunkFragmentReader)
DECLARE_REFCOUNTED_STRUCT(IChunkReaderAllowingRepair)
DECLARE_REFCOUNTED_STRUCT(IRemoteChunkReader)

DECLARE_REFCOUNTED_STRUCT(IReaderBase)
DECLARE_REFCOUNTED_STRUCT(IReaderFactory)

DECLARE_REFCOUNTED_STRUCT(IMultiReaderManager)

DECLARE_REFCOUNTED_CLASS(TTrafficMeter)

DECLARE_REFCOUNTED_STRUCT(IChunkWriterBase)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IBlockCache)
DECLARE_REFCOUNTED_STRUCT(IClientBlockCache)

DECLARE_REFCOUNTED_CLASS(TMemoryWriter)

DECLARE_REFCOUNTED_CLASS(TInputChunk)
DECLARE_REFCOUNTED_CLASS(TInputChunkSlice)

DECLARE_REFCOUNTED_STRUCT(TLegacyDataSlice)

DECLARE_REFCOUNTED_CLASS(TDataSourceDirectory)

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

DECLARE_REFCOUNTED_CLASS(IReaderMemoryManager)
DECLARE_REFCOUNTED_CLASS(TChunkReaderMemoryManager)

DECLARE_REFCOUNTED_CLASS(TChunkReplicaLocator)

struct TChunkReaderMemoryManagerOptions;

struct TUserObject;

using TRefCountedChunkMeta = TRefCountedProto<NChunkClient::NProto::TChunkMeta>;
DECLARE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TDeferredChunkMeta)

// NB: TRefCountedBlocksExt needs weak pointers support.
using TRefCountedBlocksExt = TRefCountedProto<NChunkClient::NProto::TBlocksExt>;
DECLARE_REFCOUNTED_TYPE(TRefCountedBlocksExt)

using TRefCountedMiscExt = TRefCountedProto<NChunkClient::NProto::TMiscExt>;
DECLARE_REFCOUNTED_TYPE(TRefCountedMiscExt);

using TPlacementId = TGuid;

struct TDataSliceDescriptor;

struct TInterruptDescriptor;

class TCodecStatistics;

struct TClientChunkReadOptions;

DECLARE_REFCOUNTED_CLASS(TKeySetWriter)

using TDataCenterName = std::optional<TString>;

DECLARE_REFCOUNTED_CLASS(TMemoryUsageGuard)

DECLARE_REFCOUNTED_STRUCT(IMultiReaderMemoryManager)
DECLARE_REFCOUNTED_STRUCT(IReaderMemoryManagerHost)

DECLARE_REFCOUNTED_CLASS(ICachedChunkMeta)
DECLARE_REFCOUNTED_CLASS(IClientChunkMetaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
